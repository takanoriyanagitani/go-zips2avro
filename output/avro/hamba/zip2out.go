package zip2avro

import (
	"context"
	"errors"
	"io"

	ha "github.com/hamba/avro/v2"
	ao "github.com/hamba/avro/v2/ocf"

	za "github.com/takanoriyanagitani/go-zips2avro"
	util "github.com/takanoriyanagitani/go-zips2avro/util"
)

var (
	ErrAvroSchemaParse error = errors.New("invalid schema")
)

func EncoderToOutput(
	e *ao.Encoder,
) func(za.ZipFileItemBasic) util.Io[util.Void] {
	return func(i za.ZipFileItemBasic) util.Io[util.Void] {
		return func(_ context.Context) (util.Void, error) {
			return util.Empty, e.Encode(i)
		}
	}
}

func SchemaToOutput(
	schema ha.Schema,
) func(io.Writer) func(za.ZipFileItemBasic) util.Io[util.Void] {
	return func(wtr io.Writer) func(za.ZipFileItemBasic) util.Io[util.Void] {
		enc, e := ao.NewEncoderWithSchema(
			schema,
			wtr,
		)
		if nil != e {
			return func(_ za.ZipFileItemBasic) util.Io[util.Void] {
				return func(_ context.Context) (util.Void, error) {
					return util.Empty, e
				}
			}
		}
		return EncoderToOutput(enc)
	}
}

type Config struct {
	ha.Schema
	io.Writer
}

func (c Config) ToOutput() func(za.ZipFileItemBasic) util.Io[util.Void] {
	return SchemaToOutput(c.Schema)(c.Writer)
}

type ConfigSource util.Io[Config]

func ConfigSourceNew(
	schemaSource util.Io[string],
) func(util.Io[io.Writer]) util.Io[Config] {
	return func(wsrc util.Io[io.Writer]) util.Io[Config] {
		return func(ctx context.Context) (Config, error) {
			rawSchema, re := schemaSource(ctx)
			schema, pe := ha.Parse(rawSchema)
			if nil != pe {
				pe = errors.Join(ErrAvroSchemaParse, pe)
			}
			wtr, we := wsrc(ctx)
			return Config{
				Schema: schema,
				Writer: wtr,
			}, errors.Join(re, pe, we)
		}
	}
}
