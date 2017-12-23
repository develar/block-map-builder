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
	"compress/gzip"
)

type BlockMap struct {
	Version string         `json:"version"`
	Files   []BlockMapFile `json:"files"`
}

type BlockMapFile struct {
	Name   string `json:"name"`
	Offset uint64 `json:"offset"`

	Checksums []string `json:"checksums"`
	Sizes     []int `json:"sizes"`
}

type ChunkerConfiguration struct {
	Window int
	Avg    int
	Min    int
	Max    int
}

func main() {
	log.SetPrefix(os.Args[0] + ": ")
	log.SetFlags(0)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprint(os.Stderr, "Divide in-file into variable-sized, content-defined chunks that are robust to\n")
		fmt.Fprint(os.Stderr, "insertions, deletions, and changes to in-file.\n\n")
		flag.PrintDefaults()
		os.Exit(2)
	}

	inFile := flag.String("in", "", "input file")
	outFile := flag.String("out", "", "output file")

	window := FlagBytes("window", 64, "use a rolling hash with window size `w`")
	avg := FlagBytes("avg", 16*1024, "average chunk `size`; must be a power of 2")
	min := FlagBytes("min", 8*1024, "minimum chunk `size`")
	max := FlagBytes("max", 32*1024, "maximum chunk `size`")

	flag.Parse()

	chunkerConfiguration := ChunkerConfiguration{
		Window: int(*window),
		Avg:    int(*avg),
		Min:    int(*min),
		Max:    int(*max),
	}

	if chunkerConfiguration.Min > chunkerConfiguration.Max {
		log.Fatal("-min must be <= -max")
	}
	if chunkerConfiguration.Avg&(chunkerConfiguration.Avg-1) != 0 {
		log.Fatal("-avg must be a power of two")
	}
	if chunkerConfiguration.Min < chunkerConfiguration.Window {
		log.Fatal("-min must be >= -window")
	}

	checksums, sizes := computeBlocks(*inFile, chunkerConfiguration)

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

	serializedBlockMap, err := json.Marshal(&blockMap)
	if err != nil {
		log.Fatal(err)
	}

	writeResult(*outFile, serializedBlockMap)
}
func writeResult(outFile string, serializedBlockMap []byte) {
	if outFile == "" {
		_, err := os.Stdout.Write(serializedBlockMap)
		if err != nil {
			log.Fatalf("error writing: %s", err)
		}

		return
	}

	outFileFileDescriptor, err := os.Create(outFile)
	if err != nil {
		log.Fatal(err)
	}
	defer Close(outFileFileDescriptor)

	gzipWriter, _ := gzip.NewWriterLevel(outFileFileDescriptor, gzip.BestCompression)
	_, err = gzipWriter.Write(serializedBlockMap)
	if err != nil {
		log.Fatal(err)
	}

	err = gzipWriter.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func computeBlocks(inFile string, configuration ChunkerConfiguration) ([]string, []int) {
	inputFileDescriptor, err := os.Open(inFile)
	if err != nil {
		log.Fatal(err)
	}
	defer Close(inputFileDescriptor)

	var checksums []string
	var sizes []int

	h, err := blake2b.New(&blake2b.Config{Size: 18})
	if err != nil {
		log.Fatal(err)
	}

	// Chunk and write output files.
	copyBuffer := new(bytes.Buffer)
	r := io.TeeReader(inputFileDescriptor, copyBuffer)
	c := rabin.NewChunker(rabin.NewTable(rabin.Poly64, configuration.Window), r, configuration.Min, configuration.Avg, configuration.Max)
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
		sizes = append(sizes, copyLength)

		h.Reset()
	}

	return checksums, sizes
}

// http://www.blevesearch.com/news/Deferred-Cleanup,-Checking-Errors,-and-Potential-Problems/
func Close(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Fatal(err)
	}
}