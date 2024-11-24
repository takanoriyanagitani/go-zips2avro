package rdr2names

import (
	"bufio"
	"context"
	"io"
	"iter"
	"os"

	util "github.com/takanoriyanagitani/go-zips2avro/util"
)

func ReaderToNames(rdr io.Reader) iter.Seq[string] {
	return func(yield func(string) bool) {
		var s *bufio.Scanner = bufio.NewScanner(rdr)
		for s.Scan() {
			var name string = s.Text()
			if !yield(name) {
				return
			}
		}
	}
}

func StdinToNamesSource() util.Io[iter.Seq[string]] {
	return func(_ context.Context) (iter.Seq[string], error) {
		return ReaderToNames(os.Stdin), nil
	}
}
