package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/storage"
)

var prismURL = "https://www.rsm.govt.nz/assets/Uploads/documents/prism/prism.zip"

func fetch(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Couldn't create storage client: %v", err)
		return
	}

	resp, err := http.Get(prismURL)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "%v", err)
		return
	}
	bkt := client.Bucket("nz-wireless-map")
	obj := bkt.Object("prism/today")
	objW := obj.NewWriter(context.Background())
	_, err = objW.Write(body)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "error writing to cloud storage: %v", err)
		return
	}
	if err := objW.Close(); err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "error closing cloud storage writer: %v", err)
		return
	}
	fmt.Fprintf(w, "saved to GCS, size=%v bytes\n", len(body))
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
