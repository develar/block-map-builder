package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"encoding/base64"

	"github.com/aclements/go-rabin/rabin"
	"github.com/minio/blake2b-simd"
	"encoding/json"
)

type BlockMap struct {
	Version string         `json:"version"`
	Files   []BlockMapFile `json:"files"`
}

type BlockMapFile struct {
	Name   string `json:"name"`
	Offset uint64 `json:"offset"`

	Checksums []string `json:"checksums"`
	Sizes     []uint16 `json:"sizes"`
}

func main() {
	// Parse and validate flags
	log.SetPrefix(os.Args[0] + ": ")
	log.SetFlags(0)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] in-file\n\n", os.Args[0])
		fmt.Fprint(os.Stderr, "Divide in-file into variable-sized, content-defined chunks that are robust to\n")
		fmt.Fprint(os.Stderr, "insertions, deletions, and changes to in-file.\n\n")
		flag.PrintDefaults()
		os.Exit(2)
	}

	window := FlagBytes("window", 64, "use a rolling hash with window size `w`")
	avg := FlagBytes("avg", 16*1024, "average chunk `size`; must be a power of 2")
	min := FlagBytes("min", 8*1024, "minimum chunk `size`")
	max := FlagBytes("max", 32*1024, "maximum chunk `size`")
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
	}
	if *min > *max {
		log.Fatal("-min must be <= -max")
	}
	if *avg&(*avg-1) != 0 {
		log.Fatal("-avg must be a power of two")
	}
	if *min < *window {
		log.Fatal("-min must be >= -window")
	}

	inFile := flag.Arg(0)

	f, err := os.Open(inFile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var checksums []string
	var sizes []uint16

	h, _ := blake2b.New(&blake2b.Config{Size: 18})

	// Chunk and write output files.
	copyBuffer := new(bytes.Buffer)
	r := io.TeeReader(f, copyBuffer)
	c := rabin.NewChunker(rabin.NewTable(rabin.Poly64, int(*window)), r, int(*min), int(*avg), int(*max))
	for i := 0; ; i++ {
		copyLength, err := c.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		_, err = io.CopyN(h, copyBuffer, int64(copyLength))
		if err != nil {
			log.Fatal("error writing hash")
		}

		checksums = append(checksums, base64.StdEncoding.EncodeToString(h.Sum(nil)))
		sizes = append(sizes, uint16(copyLength))

		h.Reset()
	}

	blockMap := BlockMap{
		Version: "2",
		Files: []BlockMapFile{
			{
				Name:      "file",
				Offset:    0,
				Checksums: checksums,
				Sizes: sizes,
			},
		},
	}

	b, err := json.Marshal(&blockMap)
	if err != nil {
		fmt.Println("error:", err)
	}
	os.Stdout.Write(b)
}
