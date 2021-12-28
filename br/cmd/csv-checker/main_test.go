package main

import (
	"flag"
	"path/filepath"
	"testing"
)

var (
	basedir = "/Users/knull/pingcap/tidb/br/cmd/csv-checker/csvs"
)

func initArgs(fileName string) {
	err := flag.Set("f", filepath.Join(basedir, fileName))
	if err != nil {
		panic(err)
	}
	flag.Parse()
	initCSVConfig()
}

func TestCheckCSV(t *testing.T) {
	initArgs("tt.csv")
	runCSVParser()
}

func TestCheckErrorCSV(t *testing.T) {
	initArgs("err.csv")
	runCSVParser()
}
