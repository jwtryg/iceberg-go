package iceberg

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	avro "github.com/hamba/avro/v2"
)

const (
	ADJUST_TO_UTC_PROP      = "adjust-to-utc"
	ELEMENT_ID_PROP         = "element-id"
	FIELD_ID_PROP           = "field-id"
	ICEBERG_FIELD_NAME_PROP = "iceberg-field-name"
	KEY_ID_PROP             = "key-id"
	LOGICAL_TYPE_PROP       = "logicalType"
	VALUE_ID_PROP           = "value-id"
)

func AvroToIceberg() (*Schema, error) {
	return nil, nil
}

// Convert an Iceberg schema into an Avro schema.
func IcebergToAvro(tableName string, sc *Schema) (avro.Schema, error) {
	return Visit(sc, &ToAvroVisitor{
		depth:      0,
		fieldIDs:   stack{},
		schemaName: tableName,
	})
}

type ToAvroVisitor struct {
	depth      int
	fieldIDs   stack
	schemaName string
}

func (v *ToAvroVisitor) Schema(schema *Schema, structResult avro.Schema) avro.Schema {
	return structResult
}

func (v *ToAvroVisitor) BeforeField(field NestedField) {
	v.depth++
	v.fieldIDs.Push(field.ID)
}

func (v *ToAvroVisitor) AfterField(field NestedField) {
	v.depth--
	v.fieldIDs.Pop()
}

func (v *ToAvroVisitor) Struct(st StructType, fieldResults []avro.Schema) avro.Schema {
	var err error
	var field *avro.Field
	var result *avro.RecordSchema

	name, err := v.getStructName(st)
	if err != nil {
		panic(err)
	}

	stfields := st.Fields()
	fields := make([]*avro.Field, len(fieldResults))
	for i, fieldResult := range fieldResults {
		stfield := stfields[i]
		props := map[string]any{
			"field-id": fmt.Sprintf("%d", stfield.ID),
		}

		fieldname := stfield.Name
		sanitized, err := sanitize(fieldname)
		if err != nil {
			panic(err)
		}
		if sanitized != fieldname {
			props[ICEBERG_FIELD_NAME_PROP] = fieldname
			fieldname = sanitized
		}

		opts := []avro.SchemaOption{
			avro.WithDoc(stfield.Doc),
			avro.WithProps(props),
		}
		if stfield.WriteDefault != nil {
			opts = append(opts, avro.WithDefault(stfield.WriteDefault))
		} else if !stfield.Required {
			opts = append(opts, avro.WithDefault(nil))
		}
		if field, err = avro.NewField(fieldname, fieldResult, opts...); err != nil {
			panic(err)
		}
		fields = append(fields, field)
	}

	if result, err = avro.NewRecordSchema(name, "", fields); err != nil {
		panic(err)
	}
	return result
}

func (v *ToAvroVisitor) Field(field NestedField, fieldResult avro.Schema) avro.Schema {
	var err error
	if !field.Required {
		if fieldResult, err = makeOptional(fieldResult); err != nil {
			panic(err)
		}
	}
	return fieldResult
}

func (v *ToAvroVisitor) List(list ListType, elemResult avro.Schema) avro.Schema {
	opts := []avro.SchemaOption{
		avro.WithProps(map[string]any{
			"element-id": list.ElementID,
		}),
	}

	return avro.NewArraySchema(elemResult, opts...)
}

func (v *ToAvroVisitor) Map(mapType MapType, keyResult, valueResult avro.Schema) avro.Schema {
	if keyResult.Type() == avro.String {
		return avro.NewMapSchema(
			valueResult,
			avro.WithProps(map[string]any{
				KEY_ID_PROP:   mapType.KeyID,
				VALUE_ID_PROP: mapType.ValueID,
			}),
		)
	}

	kvRecord, err := newKeyValueRecord(mapType.KeyID, mapType.ValueID, keyResult, valueResult)
	if err != nil {
		panic(err)
	}

	return avro.NewArraySchema(
		kvRecord,
		avro.WithProps(map[string]any{
			LOGICAL_TYPE_PROP: "map",
		}),
	)
}

func (v *ToAvroVisitor) Primitive(p PrimitiveType) avro.Schema {
	var err error
	var primitiveSchema avro.Schema

	switch p := p.(type) {
	case BinaryType:
		primitiveSchema = avro.NewPrimitiveSchema(avro.Bytes, nil)
	case BooleanType:
		primitiveSchema = avro.NewPrimitiveSchema(avro.Boolean, nil)
	case Float32Type:
		primitiveSchema = avro.NewPrimitiveSchema(avro.Float, nil)
	case Float64Type:
		primitiveSchema = avro.NewPrimitiveSchema(avro.Double, nil)
	case Int32Type:
		primitiveSchema = avro.NewPrimitiveSchema(avro.Int, nil)
	case Int64Type:
		primitiveSchema = avro.NewPrimitiveSchema(avro.Long, nil)
	case StringType:
		primitiveSchema = avro.NewPrimitiveSchema(avro.String, nil)
	case UUIDType:
		primitiveSchema, err = avro.NewFixedSchema("uuid_fixed", "", 16, avro.NewPrimitiveLogicalSchema(avro.UUID))
	case FixedType:
		primitiveSchema, err = avro.NewFixedSchema("fixed", "", p.len, nil)
	case DecimalType:
		primitiveSchema = avro.NewPrimitiveSchema(avro.Bytes, avro.NewDecimalLogicalSchema(p.precision, p.scale))
	case DateType:
		primitiveSchema = avro.NewPrimitiveSchema(avro.Int, avro.NewPrimitiveLogicalSchema(avro.Date))
	case TimeType:
		primitiveSchema = avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimeMicros))
	case TimestampType:
		opt := avro.WithProps(map[string]any{
			ADJUST_TO_UTC_PROP: false,
		})
		primitiveSchema = avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMillis), opt)
	case TimestampTzType:
		opt := avro.WithProps(map[string]any{
			ADJUST_TO_UTC_PROP: true,
		})
		primitiveSchema = avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMillis), opt)
	default:
		panic(fmt.Sprintf("unexpected iceberg.PrimitiveType: %#v", p))
	}

	if err != nil {
		panic(err)
	}

	return primitiveSchema
}

func makeOptional(elemResult avro.Schema) (avro.Schema, error) {
	return avro.NewUnionSchema([]avro.Schema{
		&avro.NullSchema{},
		elemResult,
	})
}

func newKeyValueRecord(keyID, valueID int, keyResult, valueResult avro.Schema) (avro.Schema, error) {
	kv := "k" + strconv.Itoa(keyID) + "_v" + strconv.Itoa(valueID)
	keySchema, err := avro.NewField("key", keyResult, avro.WithProps(map[string]any{FIELD_ID_PROP: keyID}))
	if err != nil {
		return nil, fmt.Errorf("failed to create key field: %w", err)
	}

	valueSchema, err := avro.NewField("value", valueResult, avro.WithProps(map[string]any{FIELD_ID_PROP: valueID}))
	if err != nil {
		return nil, fmt.Errorf("failed to create value field: %w", err)
	}

	rec, err := avro.NewRecordSchema(kv, "", []*avro.Field{keySchema, valueSchema})
	if err != nil {
		return nil, fmt.Errorf("failed to create record schema: %w", err)
	}

	return rec, nil
}

func (v *ToAvroVisitor) getStructName(st StructType) (string, error) {
	if v.depth == 0 {
		return v.schemaName, nil
	}

	id, err := v.fieldIDs.Peek()
	if err != nil {
		return "", fmt.Errorf("failed to get field ID for struct %v", st)
	}

	return fmt.Sprintf("r%d", id), nil
}

func sanitize(name string) (string, error) {
	if len(name) == 0 {
		return "", fmt.Errorf("name cannot be empty")
	}

	runes := []rune(name)
	sb := strings.Builder{}

	first := runes[0]
	if !(unicode.IsLetter(first) || first == '_') {
		sb.WriteString(sanitizeRune(first))
	} else {
		sb.WriteRune(first)
	}

	for _, r := range runes[1:] {
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
			sb.WriteString(sanitizeRune(r))
		} else {
			sb.WriteRune(r)
		}
	}

	return sb.String(), nil
}

func sanitizeRune(r rune) string {
	if unicode.IsDigit(r) {
		return "_" + string(r)
	}
	return "_x" + strings.ToUpper(strconv.QuoteRuneToASCII(r))
}

type stack struct {
	s []int
}

func (s *stack) Push(v int) {
	s.s = append(s.s, v)
}

func (s *stack) Peek() (int, error) {
	l := len(s.s)
	if l == 0 {
		return 0, errors.New("empty Stack")
	}

	return s.s[l-1], nil
}

func (s *stack) Pop() (int, error) {
	l := len(s.s)
	if l == 0 {
		return 0, errors.New("empty Stack")
	}

	res := s.s[l-1]
	s.s = s.s[:l-1]
	return res, nil
}
