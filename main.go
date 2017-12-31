package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"encoding/json"
	"github.com/develar/block-map-builder/blockmap"
)

func main() {
	log.SetPrefix(os.Args[0] + ": ")
	log.SetFlags(0)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprint(os.Stderr, "Generates file block map for differential update using content defined chunking (that is robust to\n")
		fmt.Fprint(os.Stderr, "insertions, deletions, and changes to input file).\n\n")
		flag.PrintDefaults()
		os.Exit(2)
	}

	inFile := flag.String("in", "", "input file")
	outFile := flag.String("out", "", "output file")

	compression := flag.String("compression", "gzip", "The compression, one of: gzip, deflate")

	window := FlagBytes("window", Bytes(blockmap.DefaultChunkerConfiguration.Window), "use a rolling hash with window size `w`")
	avg := FlagBytes("avg", Bytes(blockmap.DefaultChunkerConfiguration.Avg), "average chunk `size`; must be a power of 2")
	min := FlagBytes("min", Bytes(blockmap.DefaultChunkerConfiguration.Min), "minimum chunk `size`")
	max := FlagBytes("max", Bytes(blockmap.DefaultChunkerConfiguration.Max), "maximum chunk `size`")

	flag.Parse()

	var compressionFormat blockmap.CompressionFormat
	switch *compression {
	case "gzip":
		compressionFormat = blockmap.GZIP
	case "deflate":
		compressionFormat = blockmap.DEFLATE
	default:
		log.Fatalf("Unknown compression format %s", *compression)
	}

	chunkerConfiguration := blockmap.ChunkerConfiguration{
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

	inputInfo, err := blockmap.BuildBlockMap(*inFile, chunkerConfiguration, compressionFormat, *outFile)
	if err != nil {
		log.Fatal(err)
	}

	serializedInputInfo, err := json.Marshal(inputInfo)
	if err != nil {
		log.Fatal(err)
	}
	_, err = os.Stdout.Write(serializedInputInfo)
	if err != nil {
		log.Fatal(err)
	}
}