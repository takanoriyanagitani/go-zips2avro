package zips2avro

import (
	_ "embed"
)

type Method uint16

const (
	MethodStore   Method = 0
	MethodDeflate Method = 8
)

//go:embed zip-avro.json
var ZipFileItemBasicAvroSchema string

type ZipFileItemBasic struct {
	ModifiedUnixtimeUs int64  `avro:"modified_unixtime_us"`
	Name               string `avro:"name"`
	Comment            string `avro:"comment"`
	RawBytes           []byte `avro:"raw_bytes"`
	CompressedSize64   int64  `avro:"compressed_size_64"`
	UncompressedSize64 int64  `avro:"uncompressed_size_64"`
	CRC32              uint32 `avro:"crc_32"`
	Method             Method `avro:"method"`
}

//go:embed generic-zip.avsc
var ZipFileItemGenericAvroSchema string

type ZipFileItemGeneric map[string]any
