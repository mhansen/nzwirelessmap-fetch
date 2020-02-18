package main

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"

	"cloud.google.com/go/storage"
)

var (
	prismZipURL = flag.String("prism_zip_url", "https://www.rsm.govt.nz/assets/Uploads/documents/prism/prism.zip", "URL of zip to fetch")
	bucketName  = flag.String("bucket_name", "nz-wireless-map", "Google Cloud Storage bucket name")
)

func fetchInternal(r *http.Request) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("Couldn't create storage client: %v", err)
	}

	log.Printf("fetching %v\n", *prismZipURL)

	resp, err := http.Get(*prismZipURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	log.Printf("Headers: %+v\n", resp.Header)

	t, err := lastModifiedTime(resp)
	if err != nil {
		return err
	}
	log.Printf("Last Modified time: %v\n", t)
	tSuffix := t.Format(time.RFC3339)
	bkt := client.Bucket(*bucketName)
	blobJSONLatest := bkt.Object("prism.json/latest")
	blobJSON := bkt.Object("prism.json/" + tSuffix)
	blobCSV := bkt.Object("prism.csv/" + tSuffix)
	blobZIP := bkt.Object("prism.zip/" + tSuffix)

	// Check if we've already created prism.json/{{timestamp}}.
	// If we've already created this file, this means we can skip a bunch of work.
	// This depends on the Last-Modified-Time in RSM's web server working, but
	// it should work.
	exists, err := objectExists(ctx, blobJSON)
	if err != nil {
		return err
	}
	if exists {
		log.Printf("exiting early: we have already created %v, no need to redo", blobJSON.ObjectName())
		return nil
	}
	log.Printf("%v does not already exist: fetching...", blobJSON.ObjectName())

	// Read in the response body: now that we've confirmed this is new data, we should load it in.
	var zipTmp bytes.Buffer
	n, err := io.Copy(&zipTmp, resp.Body)
	if err != nil {
		return err
	}
	log.Printf("fetched %v bytes\n", n)

	// Save the prism.zip to a timestamped file on GCS.
	if err = writeToGCS(ctx, blobZIP, bytes.NewReader(zipTmp.Bytes())); err != nil {
		return err
	}

	// Decode the prism.zip file
	log.Println("opening zip")
	zipR, err := zip.NewReader(bytes.NewReader(zipTmp.Bytes()), int64(zipTmp.Len()))
	if err != nil {
		return fmt.Errorf("error opening zip: %v", err)
	}

	// Find prism.mdb inside the prism.zip file
	log.Println("finding prism.mdb")
	prismMDB, err := findPrismMdb(zipR)
	if err != nil {
		return fmt.Errorf("couldn't find prism.mdb: %v", err)
	}

	// Read prism.mdb into a tmpfile. mdb-sqlite requires a file: won't work with stdin.
	log.Println("opening prism.mdb")
	mdbR, err := prismMDB.Open()
	if err != nil {
		return fmt.Errorf("couldn't open prism.mdb: %v", err)
	}
	defer mdbR.Close()

	mdbTmp, err := tempFile("prism.mdb")
	if err != nil {
		return err
	}
	defer mdbTmp.Close()
	defer os.Remove(mdbTmp.Name())

	log.Println("saving prism.mdb to disk")
	n, err = io.Copy(mdbTmp, mdbR)
	log.Printf("read %v bytes from prism.mdb\n", n)
	if err != nil {
		return fmt.Errorf("couldn't read prism.mdb from zip: %v", err)
	}

	// Make an output tmpfile for the sqlite3 database. stdout isn't enough.
	tmpSqlite, err := tempFile("prism.sqlite3")
	if err != nil {
		return err
	}
	defer tmpSqlite.Close()
	defer os.Remove(tmpSqlite.Name())

	// Convert to sqlite3
	if err := mdbToSqlite(mdbTmp, tmpSqlite); err != nil {
		return err
	}

	// Query sqlite to CSV
	var tmpCSV bytes.Buffer
	if err := querySqliteToCSV(tmpSqlite, &tmpCSV); err != nil {
		return err
	}

	// Save prism.csv to GCS
	if err := writeToGCS(ctx, blobCSV, bytes.NewReader(tmpCSV.Bytes())); err != nil {
		return err
	}

	// Convert CSV to JSON
	var tmpJSON bytes.Buffer
	if err = csvToJSON(bytes.NewReader(tmpCSV.Bytes()), &tmpJSON); err != nil {
		return err
	}

	// Save JSON to GCS
	if err := writeToGCS(ctx, blobJSONLatest, bytes.NewReader(tmpJSON.Bytes())); err != nil {
		return err
	}
	// Finally save to a timestamped JSON file. This is a history, as well as a
	// way to tell if the pipeline completed end-to-end (above we check if this
	// file exists to see if we can save work).
	if err := writeToGCS(ctx, blobJSON, bytes.NewReader(tmpJSON.Bytes())); err != nil {
		return err
	}

	// Success!
	return nil
}

func objectExists(ctx context.Context, blob *storage.ObjectHandle) (bool, error) {
	attrs, err := blob.Attrs(ctx)
	if err != nil {
		log.Printf("got err getting attrs on %v: %v", blob.ObjectName(), err)
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		// We don't know if the object exists, other error getting attrs.
		return false, fmt.Errorf("couldn't get attrs on %v: %v", blob.ObjectName(), err)
	}

	log.Printf("got attrs for %v: %v", blob.ObjectName(), attrs)

	// prism.json/{{timestamp}} *does* exist already! No need to continue.
	return true, nil
}

func lastModifiedTime(resp *http.Response) (lmt time.Time, err error) {
	lm := resp.Header.Get("Last-Modified")
	log.Printf("Last Modified: %v\n", lm)
	if lmt, err = time.Parse(http.TimeFormat, lm); err != nil {
		err = fmt.Errorf("Couldn't parse Last-Modified header %q: %v", lm, err)
	}
	return
}

func mdbToSqlite(mdbTmp *os.File, tmpSqlite *os.File) error {
	// Convert to sqlite3
	cmd := exec.Command("/usr/bin/java", "-jar", "mdb-sqlite.jar", mdbTmp.Name(), tmpSqlite.Name())
	log.Printf("Converting to sqlite3: running %v\n", cmd.String())
	if javaOutput, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("couldn't read output from java: %v, output: %v", err, javaOutput)
	}

	// Analyze output with sqlite3
	analyzeCmd := exec.Command("/usr/bin/sqlite3", tmpSqlite.Name(), "analyze main;")
	log.Printf("Analyzing database in sqlite: running %v\n", analyzeCmd.String())
	if analyzeOut, err := analyzeCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("couldn't analyze db: %v, output: %v", err, analyzeOut)
	}
	return nil
}

// tempFile creates a temporary file. It's the caller's responsibility to close and delete the file.
func tempFile(pattern string) (f *os.File, err error) {
	if f, err = ioutil.TempFile(os.TempDir(), pattern); err != nil {
		err = fmt.Errorf("couldn't create temp file: %v", err)
	}
	return
}

func querySqliteToCSV(tmpSqlite *os.File, tmpCsv io.Writer) error {
	// Run SQL to ouput CSV
	sqlF, err := os.Open("select_point_to_point_links.sql")
	if err != nil {
		return err
	}

	var selectErr bytes.Buffer
	c := exec.Command("/usr/bin/sqlite3", tmpSqlite.Name())
	c.Stdin = sqlF
	c.Stdout = tmpCsv
	c.Stderr = &selectErr

	log.Printf("Extracting data from sqlite: running %v\n", c.String())
	if err := c.Run(); err != nil {
		return fmt.Errorf("couldn't select: %v, stderr: %v", err, selectErr.String())
	}
	return nil
}

func csvToJSON(tmpCsv io.Reader, tmpJSON io.Writer) error {
	var jsonErr bytes.Buffer
	c := exec.Command("/usr/bin/python3", "csv2json2.py")
	c.Stdout = tmpJSON
	c.Stdin = tmpCsv
	c.Stderr = &jsonErr
	log.Printf("Converting to JSON: running %v\n", c.String())
	if err := c.Run(); err != nil {
		return fmt.Errorf("couldn't convert to json: %v, stderr: %v", err, jsonErr.String())
	}
	return nil
}

func writeToGCS(ctx context.Context, o *storage.ObjectHandle, f io.Reader) error {
	log.Printf("writing to GCS: %v\n", o.ObjectName())
	// We've just written to most of these files, so cursor is at the end. Rewind.
	w := o.NewWriter(ctx)
	_, err := io.Copy(w, f)
	if err != nil {
		return fmt.Errorf("error writing to cloud storage: %v", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("error closing cloud storage writer: %v", err)
	}
	a := w.Attrs()
	log.Printf("finished writing %v bytes to GCS bucket: %v, name: %v\n", a.Size, a.Bucket, a.Name)
	return nil
}

func fetch(w http.ResponseWriter, r *http.Request) {
	if err := fetchInternal(r); err != nil {
		w.WriteHeader(500)
		log.Printf("%v", err)
		fmt.Fprintf(w, "/fetch failed: %v", err)
		return
	}
	log.Println("OK")
	fmt.Fprint(w, "OK")
}

func findPrismMdb(r *zip.Reader) (*zip.File, error) {
	for _, f := range r.File {
		if f.Name == "prism.mdb" {
			return f, nil
		}
	}
	return nil, errors.New("no prism.mdb found in prism.zip")
}

func main() {
	flag.Parse()
	log.Print("Fetch server started.")

	http.HandleFunc("/fetch", fetch)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
