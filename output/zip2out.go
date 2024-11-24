package output

import (
	za "github.com/takanoriyanagitani/go-zips2avro"
	util "github.com/takanoriyanagitani/go-zips2avro/util"
)

type Output func(za.ZipFileItemBasic) util.Io[util.Void]
