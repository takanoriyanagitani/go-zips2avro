package zip2items

import (
	"archive/zip"
	"bytes"
	"io"
	"iter"
	"os"

	za "github.com/takanoriyanagitani/go-zips2avro"
)

func ZipToItems(
	zfile *zip.Reader,
	buf *bytes.Buffer,
	zipName string,
) iter.Seq2[za.ZipFileItemGeneric, error] {
	return func(yield func(za.ZipFileItemGeneric, error) bool) {
		for _, file := range zfile.File {
			generic := map[string]any{
				"ModifiedUnixtimeUs": file.Modified.UnixMicro(),
				"Name":               file.Name,
				"Comment":            file.Comment,
				"RawBytes":           nil,
				"CompressedSize64":   int64(file.CompressedSize64),
				"UncompressedSize64": int64(file.UncompressedSize64),
				"CRC32":              int(file.CRC32),
				"Method":             int(file.Method),
				"ZipFilename":        zipName,
			}
			raw, e := file.OpenRaw()
			if nil == e {
				buf.Reset()
				_, e = io.Copy(buf, raw)
				if nil == e {
					generic["RawBytes"] = buf.Bytes()
				}
			}

			if !yield(generic, e) {
				return
			}
		}
	}
}

func FileLikeToItems(
	flike io.ReaderAt,
	fsize int64,
	buf *bytes.Buffer,
	zipName string,
) iter.Seq2[za.ZipFileItemGeneric, error] {
	zrdr, e := zip.NewReader(flike, fsize)
	if nil != e {
		return func(yield func(za.ZipFileItemGeneric, error) bool) {
			yield(za.ZipFileItemGeneric{}, e)
		}
	}
	return ZipToItems(zrdr, buf, zipName)
}

func FileToItems(
	file *os.File,
	buf *bytes.Buffer,
	zipName string,
) iter.Seq2[za.ZipFileItemGeneric, error] {
	fstat, e := file.Stat()
	if nil != e {
		return func(yield func(za.ZipFileItemGeneric, error) bool) {
			yield(za.ZipFileItemGeneric{}, e)
		}
	}
	var fsize int64 = fstat.Size()
	return FileLikeToItems(
		file,
		fsize,
		buf,
		zipName,
	)
}

func NamesToItems(
	zipNames iter.Seq[string],
) iter.Seq2[za.ZipFileItemGeneric, error] {
	return func(yield func(za.ZipFileItemGeneric, error) bool) {
		var buf bytes.Buffer
		for zipName := range zipNames {
			file, e := os.Open(zipName)
			if nil != e {
				if !yield(za.ZipFileItemGeneric{}, e) {
					return
				}
				continue
			}

			var ok bool = func() bool {
				defer file.Close()

				var pairs iter.Seq2[za.ZipFileItemGeneric, error] = FileToItems(
					file,
					&buf,
					zipName,
				)
				for item, e := range pairs {
					if !yield(item, e) {
						return false
					}
				}

				return true
			}()

			if !ok {
				return
			}
		}
	}
}
