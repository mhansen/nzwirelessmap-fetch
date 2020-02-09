package main

import (
	"archive/zip"
	"context"
	"errors"
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

var prismURL = "https://www.rsm.govt.nz/assets/Uploads/documents/prism/prism.zip"

func fetchInternal(r *http.Request) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("Couldn't create storage client: %v", err)
	}

	log.Printf("fetching %v\n", prismURL)

	zipTmp, err := ioutil.TempFile(os.TempDir(), "prism-zip")
	if err != nil {
		return fmt.Errorf("couldn't create temp file: %v", err)
	}
	defer zipTmp.Close()
	defer os.Remove(zipTmp.Name())

	resp, err := http.Get(prismURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	n, err := io.Copy(zipTmp, resp.Body)
	if err != nil {
		return err
	}
	log.Printf("fetched %v bytes\n", n)
	bkt := client.Bucket("nz-wireless-map")
	t := time.Now().UTC()
	err = writeToGCS(ctx, bkt.Object("prism.zip/"+t.Format(time.RFC3339)), zipTmp)
	if err != nil {
		return err
	}

	log.Println("opening zip")
	zipR, err := zip.OpenReader(zipTmp.Name())
	if err != nil {
		return fmt.Errorf("error opening zip: %v", err)
	}
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

	mdbTmp, err := ioutil.TempFile(os.TempDir(), "prism-mdb")
	if err != nil {
		return fmt.Errorf("couldn't create temp file: %v", err)
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
	tmpSqlite, err := ioutil.TempFile(os.TempDir(), "prism-sqlite3")
	if err != nil {
		return fmt.Errorf("couldn't create temp file: %v", err)
	}
	defer tmpSqlite.Close()
	defer os.Remove(tmpSqlite.Name())

	// Convert to sqlite3
	cmd := exec.Command("/usr/bin/java", "-jar", "mdb-sqlite.jar", mdbTmp.Name(), tmpSqlite.Name())
	log.Printf("Converting to sqlite3: running %v\n", cmd.String())
	javaOutput, err := cmd.CombinedOutput()
	if err != nil {
		log.Println("java output:")
		log.Println(javaOutput)
		return fmt.Errorf("couldn't read output from java: %v", err)
	}

	// Analyze output with sqlite3
	analyzeCmd := exec.Command("/usr/bin/sqlite3", tmpSqlite.Name(), "analyze main;")
	analyzeOut, err := analyzeCmd.CombinedOutput()
	if err != nil {
		log.Println("sqlite3 output:")
		log.Println(analyzeOut)
		return fmt.Errorf("couldn't analyze db: %v", err)
	}

	// Run SQL to ouput CSV
	sqlF, err := os.Open("select_point_to_point_links.sql")
	if err != nil {
		return err
	}

	tmpCsv, err := ioutil.TempFile(os.TempDir(), "prism-csv")
	if err != nil {
		return fmt.Errorf("couldn't create temp file: %v", err)
	}
	defer tmpCsv.Close()
	defer os.Remove(tmpCsv.Name())

	selectCmd := exec.Command("/usr/bin/sqlite3", tmpSqlite.Name())
	selectCmd.Stdin = sqlF
	selectCmd.Stdout = tmpCsv

	err = selectCmd.Run()
	if err != nil {
		return fmt.Errorf("couldn't select: %v", err)
	}

	// Save CSV to GCS?
	err = writeToGCS(ctx, bkt.Object("prism-csv/"+t.Format(time.RFC3339)), tmpCsv)
	if err != nil {
		return err
	}

	// Convert CSV to JSON
	tmpJSON, err := ioutil.TempFile(os.TempDir(), "prism-json")
	if err != nil {
		return fmt.Errorf("couldn't create temp file: %v", err)
	}
	defer tmpJSON.Close()
	defer os.Remove(tmpJSON.Name())

	jsonCmd := exec.Command("/usr/bin/python3", "csv2json2.py")
	jsonCmd.Stdout = tmpJSON
	jsonCmd.Stdin = tmpCsv

	// Save JSON to GCS?
	err = writeToGCS(ctx, bkt.Object("prism-json/"+t.Format(time.RFC3339)), tmpJSON)
	if err != nil {
		return err
	}

	// Notify?
	// Save to a well-known location, maybe?
	// Update a symlink?

	return nil
}

func writeToGCS(ctx context.Context, o *storage.ObjectHandle, r io.Reader) error {
	log.Printf("writing to GCS: %v\n", o.ObjectName())
	w := o.NewWriter(ctx)
	_, err := io.Copy(w, r)
	if err != nil {
		return fmt.Errorf("error writing to cloud storage: %v", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("error closing cloud storage writer: %v", err)
	}
	log.Printf("finished writing to GCS: %v\n", o.ObjectName())
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
	log.Print("Fetch server started.")

	http.HandleFunc("/fetch", fetch)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
