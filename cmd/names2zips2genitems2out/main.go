package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"

	za "github.com/takanoriyanagitani/go-zips2avro"
	util "github.com/takanoriyanagitani/go-zips2avro/util"

	bz "github.com/takanoriyanagitani/go-zips2avro/zip/generic/zip2items"
	nr "github.com/takanoriyanagitani/go-zips2avro/zip/names/reader"

	ah "github.com/takanoriyanagitani/go-zips2avro/output/avro/hamba"

	an "github.com/takanoriyanagitani/go-zips2avro/app/names2zips2genitems2out"
)

func GetEnvByKeyNew(key string) util.Io[string] {
	return func(_ context.Context) (string, error) {
		val, ok := os.LookupEnv(key)
		if !ok {
			return "", fmt.Errorf("env var missing: %s", key)
		}
		return val, nil
	}
}

var zipNames util.Io[iter.Seq[string]] = nr.StdinToNamesSource()

var names2items func(
	iter.Seq[string],
) iter.Seq2[za.ZipFileItemGeneric, error] = bz.NamesToItems

var schema util.Io[string] = func(_ context.Context) (string, error) {
	return za.ZipFileItemGenericAvroSchema, nil
}

var flushOnDone chan struct{}
var writer util.Io[io.Writer] = func(_ context.Context) (io.Writer, error) {
	var bw *bufio.Writer = bufio.NewWriter(os.Stdout)

	go func() {
		<-flushOnDone

		e := bw.Flush()
		if nil != e {
			log.Printf("unable to flush: %v\n", e)
		}
	}()

	return bw, nil
}

var config util.Io[ah.ConfigA[za.ZipFileItemGeneric]] = ah.
	ConfigSourceNewA[za.ZipFileItemGeneric](schema)(writer)

var app util.Io[an.App[za.ZipFileItemGeneric]] = util.Bind(
	config,
	func(
		cfg ah.ConfigA[za.ZipFileItemGeneric],
	) util.Io[an.App[za.ZipFileItemGeneric]] {
		return func(_ context.Context) (an.App[za.ZipFileItemGeneric], error) {
			return an.App[za.ZipFileItemGeneric]{
				NamesSource:  zipNames,
				NamesToItems: names2items,
				ItemToOutput: cfg.ToOutput(),
			}, nil
		}
	},
)

func sub(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	a, e := app(ctx)
	if nil != e {
		return e
	}

	var outputAll util.Io[util.Void] = a.ToOutputAll()
	_, e = outputAll(ctx)
	return e
}

func main() {
	e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}