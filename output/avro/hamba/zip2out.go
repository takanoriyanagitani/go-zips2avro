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

func EncoderToOutputA[T any](
	e *ao.Encoder,
) func(T) util.Io[util.Void] {
	return func(i T) util.Io[util.Void] {
		return func(_ context.Context) (util.Void, error) {
			return util.Empty, e.Encode(i)
		}
	}
}

func SchemaToOutputA[T any](
	schema ha.Schema,
) func(io.Writer) func(T) util.Io[util.Void] {
	return func(wtr io.Writer) func(T) util.Io[util.Void] {
		enc, e := ao.NewEncoderWithSchema(
			schema,
			wtr,
		)
		if nil != e {
			return func(_ T) util.Io[util.Void] {
				return func(_ context.Context) (util.Void, error) {
					return util.Empty, e
				}
			}
		}
		return EncoderToOutputA[T](enc)
	}
}

type ConfigA[T any] struct {
	ha.Schema
	io.Writer
}

func (c ConfigA[T]) ToOutput() func(T) util.Io[util.Void] {
	return SchemaToOutputA[T](c.Schema)(c.Writer)
}

type ConfigSourceA[T any] util.Io[ConfigA[T]]

func ConfigSourceNewA[T any](
	schemaSource util.Io[string],
) func(util.Io[io.Writer]) util.Io[ConfigA[T]] {
	return func(wsrc util.Io[io.Writer]) util.Io[ConfigA[T]] {
		return func(ctx context.Context) (ConfigA[T], error) {
			rawSchema, re := schemaSource(ctx)
			schema, pe := ha.Parse(rawSchema)
			if nil != pe {
				pe = errors.Join(ErrAvroSchemaParse, pe)
			}
			wtr, we := wsrc(ctx)
			return ConfigA[T]{
				Schema: schema,
				Writer: wtr,
			}, errors.Join(re, pe, we)
		}
	}
}

func EncoderToOutput(
	e *ao.Encoder,
) func(za.ZipFileItemBasic) util.Io[util.Void] {
	return EncoderToOutputA[za.ZipFileItemBasic](e)
}

func SchemaToOutput(
	schema ha.Schema,
) func(io.Writer) func(za.ZipFileItemBasic) util.Io[util.Void] {
	return SchemaToOutputA[za.ZipFileItemBasic](schema)
}

type Config ConfigA[za.ZipFileItemBasic]

func (c Config) ToOutput() func(za.ZipFileItemBasic) util.Io[util.Void] {
	return ConfigA[za.ZipFileItemBasic](c).ToOutput()
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
