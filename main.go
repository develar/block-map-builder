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
	"crypto/sha512"
	"compress/flate"
	"encoding/binary"
	"hash"
)

type BlockMap struct {
	Version string         `json:"version"`
	Files   []BlockMapFile `json:"files"`
}

type InputFileInfo struct {
	Size   int  `json:"size"`
	Sha512 string `json:"sha512"`

	BlockMapSize *int `json:"blockMapSize,omitempty"`

	hash *hash.Hash
}

type BlockMapFile struct {
	Name   string `json:"name"`
	Offset uint64 `json:"offset"`

	Checksums []string `json:"checksums"`
	Sizes     []int    `json:"sizes"`
}

type ChunkerConfiguration struct {
	Window int
	Avg    int
	Min    int
	Max    int
}

type CompressionFormat int

const (
	GZIP    = 0
	DEFLATE = 1
)

var defaultChunkerConfiguration = ChunkerConfiguration{
	Window: 64,
	Avg: 16*1024,
	Min: 8*1024,
	Max: 32*1024,
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

	compression := flag.String("compression", "gzip", "The compression, one of: gzip, deflate")
	isAppend := flag.Bool("append", false, "Whether to create a new file or append to input file.")

	window := FlagBytes("window", 64, "use a rolling hash with window size `w`")
	avg := FlagBytes("avg", 16*1024, "average chunk `size`; must be a power of 2")
	min := FlagBytes("min", 8*1024, "minimum chunk `size`")
	max := FlagBytes("max", 32*1024, "maximum chunk `size`")

	flag.Parse()

	var compressionFormat CompressionFormat
	switch *compression {
	case "gzip":
		compressionFormat = GZIP
	case "deflate":
		compressionFormat = DEFLATE
	default:
		log.Fatalf("Unknown compression format %s", *compression)
	}

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

	inputInfo := BuildBlockMap(*inFile, chunkerConfiguration, *isAppend, compressionFormat, *outFile)

	serializedInputInfo, err := json.Marshal(inputInfo)
	if err != nil {
		log.Fatal(err)
	}
	_, err = os.Stdout.Write(serializedInputInfo)
	if err != nil {
		log.Fatal(err)
	}
}

func BuildBlockMap(inFile string, chunkerConfiguration ChunkerConfiguration, isAppend bool, compressionFormat CompressionFormat, outFile string) InputFileInfo {
	checksums, sizes, inputInfo := computeBlocks(inFile, chunkerConfiguration)
	blockMap := BlockMap{
		Version: "2",
		Files: []BlockMapFile{
			{
				Name:      "file",
				Offset:    0,
				Checksums: checksums,
				Sizes:     sizes,
			},
		},
	}

	serializedBlockMap, err := json.Marshal(&blockMap)
	if err != nil {
		log.Fatal(err)
	}

	if isAppend {
		archiveSize := appendResult(serializedBlockMap, inFile, compressionFormat, inputInfo.hash)
		inputInfo.Size += archiveSize + 4
		inputInfo.BlockMapSize = &archiveSize
	} else {
		writeResult(serializedBlockMap, outFile, compressionFormat)
	}

	inputInfo.Sha512 = base64.StdEncoding.EncodeToString((*inputInfo.hash).Sum(nil))
	return inputInfo
}

func appendResult(data []byte, inFile string, compressionFormat CompressionFormat, hash *hash.Hash) int {
	archiveBuffer := new(bytes.Buffer)
	archiveData(data, compressionFormat, archiveBuffer)
	outFileDescriptor, err := os.OpenFile(inFile, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer Close(outFileDescriptor)

	archiveSize := archiveBuffer.Len()
	_, err = io.Copy(outFileDescriptor, io.TeeReader(archiveBuffer, *hash))
	if err != nil {
		log.Fatal(err)
	}

	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(archiveSize))
	_, err = outFileDescriptor.Write(b)
	if err != nil {
		log.Fatal(err)
	}

	_, err = (*hash).Write(b)
	if err != nil {
		log.Fatal(err)
	}

	return archiveSize
}

func writeResult(data []byte, outFile string, compressionFormat CompressionFormat) {
	if outFile == "" {
		_, err := os.Stdout.Write(data)
		if err != nil {
			log.Fatalf("error writing: %s", err)
		}

		return
	}

	outFileDescriptor, err := os.Create(outFile)
	if err != nil {
		log.Fatal(err)
	}
	defer Close(outFileDescriptor)

	archiveData(data, compressionFormat, outFileDescriptor)
}

func archiveData(data []byte, compressionFormat CompressionFormat, destinationWriter io.Writer) {
	var archiveWriter io.WriteCloser
	var err error
	if compressionFormat == DEFLATE {
		archiveWriter, err = flate.NewWriter(destinationWriter, flate.BestCompression)
	} else {
		archiveWriter, err = gzip.NewWriterLevel(destinationWriter, gzip.BestCompression)
	}
	if err != nil {
		log.Fatal(err)
	}

	defer Close(archiveWriter)

	_, err = archiveWriter.Write(data)
	if err != nil {
		log.Fatal(err)
	}
}

func computeBlocks(inFile string, configuration ChunkerConfiguration) ([]string, []int, InputFileInfo) {
	inputFileDescriptor, err := os.Open(inFile)
	if err != nil {
		log.Fatal(err)
	}
	defer Close(inputFileDescriptor)

	var checksums []string
	var sizes []int

	chunkHash, err := blake2b.New(&blake2b.Config{Size: 18})
	if err != nil {
		log.Fatal(err)
	}

	inputHash := sha512.New()

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

		rr := io.TeeReader(io.LimitReader(copyBuffer, int64(copyLength)), inputHash)
		_, err = io.Copy(chunkHash, rr)
		if err != nil {
			log.Fatal("error writing hash")
		}

		checksums = append(checksums, base64.StdEncoding.EncodeToString(chunkHash.Sum(nil)))
		sizes = append(sizes, copyLength)

		chunkHash.Reset()
	}

	inputFileStat, err := inputFileDescriptor.Stat()
	if err != nil {
		log.Fatal(err)
	}

	sum := 0
	for _, s := range sizes {
		sum += s
	}

	fileSize := int(inputFileStat.Size())
	if sum != fileSize {
		log.Fatalf("Expected size sum: %d. Actual: %d", fileSize, sum)
	}

	return checksums, sizes, InputFileInfo{
		Size: fileSize,
		hash: &inputHash,
	}
}

// http://www.blevesearch.com/news/Deferred-Cleanup,-Checking-Errors,-and-Potential-Problems/
func Close(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Fatal(err)
	}
}
