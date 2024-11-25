package zipnames2items2out

import (
	za "github.com/takanoriyanagitani/go-zips2avro"
	util "github.com/takanoriyanagitani/go-zips2avro/util"

	ga "github.com/takanoriyanagitani/go-zips2avro/app/names2zips2genitems2out"
)

type App ga.App[za.ZipFileItemBasic]

func (a App) ToOutputAll() util.Io[util.Void] {
	return ga.App[za.ZipFileItemBasic](a).ToOutputAll()
}
