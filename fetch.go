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

	zipTmp, err := tempFile("prism.zip")
	if err != nil {
		return err
	}
	defer zipTmp.Close()
	defer os.Remove(zipTmp.Name())

	resp, err := http.Get(*prismZipURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	log.Printf("Headers: %+v\n", resp.Header)

	lmt, err := lastModifiedTime(resp)
	if err != nil {
		return err
	}
	log.Printf("Last Modified time: %v\n", lmt)

	n, err := io.Copy(zipTmp, resp.Body)
	if err != nil {
		return err
	}
	log.Printf("fetched %v bytes\n", n)

	bkt := client.Bucket(*bucketName)
	t := lmt
	err = writeToGCS(ctx, bkt.Object("prism.zip/"+t.Format(time.RFC3339)), zipTmp)
	if err != nil {
		return err
	}

	log.Println("opening zip")
	zipR, err := zip.OpenReader(zipTmp.Name())
	if err != nil {
		return fmt.Errorf("error opening zip: %v", err)
	}
	defer zipR.Close()

	log.Println("finding prism.mdb")
	prismMDB, err := findPrismMdb(zipR)
	if err != nil {
		return fmt.Errorf("couldn't find prism.mdb: %v", err)
	}

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

	// Make an output tmpfile
	tmpSqlite, err := tempFile("prism.sqlite3")
	if err != nil {
		return err
	}
	defer tmpSqlite.Close()
	defer os.Remove(tmpSqlite.Name())

	// Convert to sqlite3
	err = mdbToSqlite(mdbTmp, tmpSqlite)
	if err != nil {
		return err
	}

	tmpCSV, err := tempFile("prism.csv")
	if err != nil {
		return err
	}
	defer tmpCSV.Close()
	defer os.Remove(tmpCSV.Name())

	err = querySqliteToCSV(tmpSqlite, tmpCSV)
	if err != nil {
		return err
	}

	// Rewind, ready to pipe in to next command.
	_, err = tmpCSV.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	// Save CSV to GCS
	err = writeToGCS(ctx, bkt.Object("prism.csv/"+t.Format(time.RFC3339)), tmpCSV)
	if err != nil {
		return err
	}

	// Rewind again, ready to pipe in to next command.
	_, err = tmpCSV.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	// Convert CSV to JSON
	tmpJSON, err := tempFile("prism.json")
	if err != nil {
		return err
	}
	defer tmpJSON.Close()
	defer os.Remove(tmpJSON.Name())

	err = csvToJSON(tmpCSV, tmpJSON)
	if err != nil {
		return err
	}

	// Save JSON to GCS
	err = writeToGCS(ctx, bkt.Object("prism.json/"+t.Format(time.RFC3339)), tmpJSON)
	if err != nil {
		return err
	}
	err = writeToGCS(ctx, bkt.Object("prism.json/latest"), tmpJSON)
	if err != nil {
		return err
	}

	return nil
}

func lastModifiedTime(resp *http.Response) (lmt time.Time, err error) {
	lm := resp.Header.Get("Last-Modified")
	log.Printf("Last Modified: %v\n", lm)
	lmt, err = time.Parse(http.TimeFormat, lm)
	if err != nil {
		err = fmt.Errorf("Couldn't parse Last-Modified header %q: %v", lm, err)
	}
	return
}

func mdbToSqlite(mdbTmp *os.File, tmpSqlite *os.File) error {
	// Convert to sqlite3
	cmd := exec.Command("/usr/bin/java", "-jar", "mdb-sqlite.jar", mdbTmp.Name(), tmpSqlite.Name())
	log.Printf("Converting to sqlite3: running %v\n", cmd.String())
	javaOutput, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("couldn't read output from java: %v, output: %v", err, javaOutput)
	}

	// Analyze output with sqlite3
	analyzeCmd := exec.Command("/usr/bin/sqlite3", tmpSqlite.Name(), "analyze main;")
	analyzeOut, err := analyzeCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("couldn't analyze db: %v, output: %v", err, analyzeOut)
	}
	return nil
}

// tempFile creates a temporary file. It's the caller's responsibility to close and delete the file.
func tempFile(pattern string) (f *os.File, err error) {
	f, err = ioutil.TempFile(os.TempDir(), pattern)
	if err != nil {
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
	selectCmd := exec.Command("/usr/bin/sqlite3", tmpSqlite.Name())
	selectCmd.Stdin = sqlF
	selectCmd.Stdout = tmpCsv
	selectCmd.Stderr = &selectErr

	err = selectCmd.Run()
	if err != nil {
		return fmt.Errorf("couldn't select: %v, stderr: %v", err, selectErr.String())
	}
	return nil
}

func csvToJSON(tmpCsv io.Reader, tmpJSON io.Writer) error {
	var jsonErr bytes.Buffer
	jsonCmd := exec.Command("/usr/bin/python3", "csv2json2.py")
	jsonCmd.Stdout = tmpJSON
	jsonCmd.Stdin = tmpCsv
	jsonCmd.Stderr = &jsonErr
	err := jsonCmd.Run()
	if err != nil {
		return fmt.Errorf("couldn't convert to json: %v, stderr: %v", err, jsonErr.String())
	}
	return nil
}

func writeToGCS(ctx context.Context, o *storage.ObjectHandle, f *os.File) error {
	log.Printf("writing to GCS: %v\n", o.ObjectName())
	// We've just written to most of these files, so cursor is at the end. Rewind.
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	w := o.NewWriter(ctx)
	_, err = io.Copy(w, f)
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

func findPrismMdb(r *zip.ReadCloser) (*zip.File, error) {
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
