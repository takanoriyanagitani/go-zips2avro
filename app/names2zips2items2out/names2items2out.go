package zipnames2items2out

import (
	"context"
	"iter"

	za "github.com/takanoriyanagitani/go-zips2avro"
	util "github.com/takanoriyanagitani/go-zips2avro/util"
)

type App struct {
	NamesSource  util.Io[iter.Seq[string]]
	NamesToItems func(iter.Seq[string]) iter.Seq2[za.ZipFileItemBasic, error]
	ItemToOutput func(za.ZipFileItemBasic) util.Io[util.Void]
}

func (a App) ToOutputAll() util.Io[util.Void] {
	return func(ctx context.Context) (util.Void, error) {
		names, e := a.NamesSource(ctx)
		if nil != e {
			return util.Empty, e
		}

		var items iter.Seq2[za.ZipFileItemBasic, error] = a.NamesToItems(
			names,
		)

		for item, e := range items {
			select {
			case <-ctx.Done():
				return util.Empty, ctx.Err()
			default:
			}

			if nil != e {
				return util.Empty, e
			}

			_, e = a.ItemToOutput(item)(ctx)
			if nil != e {
				return util.Empty, e
			}
		}
		return util.Empty, nil
	}
}
