package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"io"
	"log"
	"os"
	"strings"
)

var (
	fname = flag.String("f", "/Users/knull/pingcap/tidb/testcsvs/1.csv", "csv file name")
	sep   = flag.String("s", "|#|", "separator of field")
	term  = flag.String("t", "|+|\n", "terminator of line")
	cfg   config.CSVConfig
)

const (
	block_size = 16 * 1024 * 1024
)

func initCSVConfig() {
	cfg = config.CSVConfig{
		Separator:       strings.TrimSpace(*sep),
		Delimiter:       ``,
		Terminator:      strings.TrimSpace(*term),
		Header:          true,
		NotNull:         false,
		Null:            `\N`,
		BackslashEscape: false,
		TrimLastSep:     false,
	}
	// check terminator;
	if strings.HasSuffix(cfg.Terminator, "\\n") {
		str := strings.TrimRight(cfg.Terminator, "\\n")
		str = str + "\n"
		cfg.Terminator = str
	}
}

func runCSVParser() {
	log.Println(cfg)
	fp, err := os.Open(*fname)
	if err != nil {
		log.Fatalf("open file '%s' err:%s.", *fname, err.Error())
	}
	defer fp.Close()
	w := worker.NewPool(context.TODO(), 1, "test")
	parser, err := mydump.NewCSVParser(&cfg, fp, block_size, w, true, nil)
	if err != nil {
		log.Fatalf("NewCSVParser err:%s.", err.Error())
	}
	defer parser.Close()
	var errMsgs strings.Builder
	errCnt := 0
	colN := 0
	for {
		err = parser.ReadRow()
		if err != nil {
			if !errors.ErrorEqual(err, io.EOF) {
				errMsgs.WriteString(fmt.Sprintf("\tReadUntilTerminator err(rowid %d):%s.\n", parser.LastRow().RowID, err.Error()))
				errCnt++
				continue
			} else {
				log.Println("run success .finished.")
				break
			}
		}
		if colN == 0 {
			colN = len(parser.Columns())
		}
		lrow := parser.LastRow()
		if colN != len(lrow.Row) {
			errMsgs.WriteString(fmt.Sprintf("line %d - %d;\n", lrow.RowID+1, len(lrow.Row)))
			errCnt++
			continue
		}
	}
	log.Printf(" check finished....\n")
	if errCnt == 0 {
		return
	}
	log.Printf(" %d lines not equal to header count(%d)\n%s.\n", errCnt, len(parser.Columns()), errMsgs.String())
	log.Printf("Summary total error %d", errCnt)
}

func main() {
	flag.Parse()
	initCSVConfig()
	runCSVParser()
}
