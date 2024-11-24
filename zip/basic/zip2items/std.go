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
) iter.Seq2[za.ZipFileItemBasic, error] {
	return func(yield func(za.ZipFileItemBasic, error) bool) {
		for _, file := range zfile.File {
			basic := za.ZipFileItemBasic{
				ModifiedUnixtimeUs: file.Modified.UnixMicro(),
				Name:               file.Name,
				Comment:            file.Comment,
				RawBytes:           nil,
				CompressedSize64:   int64(file.CompressedSize64),
				UncompressedSize64: int64(file.UncompressedSize64),
				CRC32:              file.CRC32,
				Method:             za.Method(file.Method),
			}
			raw, e := file.OpenRaw()
			if nil == e {
				buf.Reset()
				_, e = io.Copy(buf, raw)
				if nil == e {
					basic.RawBytes = buf.Bytes()
				}
			}

			if !yield(basic, e) {
				return
			}
		}
	}
}

func FileLikeToItems(
	flike io.ReaderAt,
	fsize int64,
	buf *bytes.Buffer,
) iter.Seq2[za.ZipFileItemBasic, error] {
	zrdr, e := zip.NewReader(flike, fsize)
	if nil != e {
		return func(yield func(za.ZipFileItemBasic, error) bool) {
			yield(za.ZipFileItemBasic{}, e)
		}
	}
	return ZipToItems(zrdr, buf)
}

func FileToItems(
	file *os.File,
	buf *bytes.Buffer,
) iter.Seq2[za.ZipFileItemBasic, error] {
	fstat, e := file.Stat()
	if nil != e {
		return func(yield func(za.ZipFileItemBasic, error) bool) {
			yield(za.ZipFileItemBasic{}, e)
		}
	}
	var fsize int64 = fstat.Size()
	return FileLikeToItems(
		file,
		fsize,
		buf,
	)
}

func NamesToItems(
	zipNames iter.Seq[string],
) iter.Seq2[za.ZipFileItemBasic, error] {
	return func(yield func(za.ZipFileItemBasic, error) bool) {
		var buf bytes.Buffer
		for zipName := range zipNames {
			file, e := os.Open(zipName)
			if nil != e {
				if !yield(za.ZipFileItemBasic{}, e) {
					return
				}
				continue
			}

			var ok bool = func() bool {
				defer file.Close()

				var pairs iter.Seq2[za.ZipFileItemBasic, error] = FileToItems(
					file,
					&buf,
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
