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

func AvroToIceberg(schema avro.Schema) (*Schema, error) {
	var err error
	converter := &toIcebergConverter{
		idCounter: 0,
	}

	recordSchema, ok := schema.(*avro.RecordSchema)
	if !ok {
		return nil, errors.New("schema must be a record")
	}

	fields := make([]NestedField, len(recordSchema.Fields()))
	for i, field := range recordSchema.Fields() {
		if fields[i], err = converter.convertField(field); err != nil {
			return nil, err
		}
	}

	return NewSchema(0, fields...), nil
}

// resolveUnion resolves a union schema.
// We only support nullable unions, which are uniuns
// of a null schema and another schema.
// Returns the resolved (or unwrapped) avro type,
// a flag indicating if the type is required, and an error.
func resolveUnion(avsc avro.Schema) (avro.Schema, bool, error) {
	var union *avro.UnionSchema
	var ok bool
	required := true

	if union, ok = avsc.(*avro.UnionSchema); !ok {
		return avsc, required, nil
	}

	types := union.Types()
	switch len(types) {
	case 0:
		return nil, false, errors.New("empty union schema")
	case 1:
		return types[0], true, nil
	case 2:
		if types[0].Type() == avro.Null {
			return types[1], false, nil
		}
		if types[1].Type() == avro.Null {
			return types[0], false, nil
		}
		return nil, false, errors.New("invalid union schema")
	default:
		return nil, false, errors.New("unsupported union schema")
	}
}

type toIcebergConverter struct {
	idCounter int
}

func (c *toIcebergConverter) getElementID(avsc *avro.ArraySchema) int {
	if elemID := avsc.Prop(ELEMENT_ID_PROP); elemID != nil {
		return int(elemID.(float64))
	} else {
		return c.allocateId()
	}
}

func (c *toIcebergConverter) getKeyID(avsc *avro.MapSchema) int {
	if keyID := avsc.Prop(KEY_ID_PROP); keyID != nil {
		return int(keyID.(float64))
	} else {
		return c.allocateId()
	}
}

func (c *toIcebergConverter) getValueID(avsc *avro.MapSchema) int {
	if valueID := avsc.Prop(VALUE_ID_PROP); valueID != nil {
		return int(valueID.(float64))
	} else {
		return c.allocateId()
	}
}

func (c *toIcebergConverter) getFieldID(field *avro.Field) int {
	if fieldID := field.Prop(FIELD_ID_PROP); fieldID != nil {
		return int(fieldID.(float64))
	} else {
		return c.allocateId()
	}
}

func (c *toIcebergConverter) allocateId() int {
	current := c.idCounter
	c.idCounter += 1
	return current
}

func (c *toIcebergConverter) convertSchema(avsc avro.Schema) (Type, error) {
	var err error
	var converted Type

	if logtyp, ok := avsc.(avro.LogicalTypeSchema); ok {
		if logsc := logtyp.Logical(); logsc != nil {
			return c.convertLogicalSchema(avsc, logsc)
		}
	}

	switch avsc.Type() {
	case avro.Boolean:
		converted = BooleanType{}
	case avro.Bytes:
		converted = BinaryType{}
	case avro.Double:
		converted = Float64Type{}
	case avro.Float:
		converted = Float32Type{}
	case avro.Int:
		converted = Int32Type{}
	case avro.Long:
		converted = Int64Type{}
	case avro.String:
		converted = StringType{}
	case avro.Enum:
		converted = StringType{}
	case avro.Array:
		converted, err = c.convertArray(avsc.(*avro.ArraySchema))
	case avro.Fixed:
		converted, err = c.convertFixed(avsc.(*avro.FixedSchema))
	case avro.Map:
		converted, err = c.convertMap(avsc.(*avro.MapSchema))
	case avro.Record:
		converted, err = c.convertRecord(avsc.(*avro.RecordSchema))
	default:
		err = fmt.Errorf("unexpected avro.Type: %#v", avsc.Type())
	}

	if err != nil {
		return nil, err
	}

	return converted, nil
}

func (c *toIcebergConverter) convertLogicalSchema(avsc avro.Schema, logsc avro.LogicalSchema) (Type, error) {
	var result Type
	t := avsc.Type()
	lt := logsc.Type()

	switch t {
	case avro.Fixed:
		if lt == avro.UUID {
			result = UUIDType{}
		}
		if lt == avro.Decimal {
			result = DecimalType{
				precision: avsc.(*avro.FixedSchema).Prop("precision").(int),
				scale:     avsc.(*avro.FixedSchema).Prop("scale").(int),
			}
		}
	case avro.Int:
		if lt == avro.Date {
			result = DateType{}
		}
	case avro.Long:
		{
			if lt == avro.TimeMicros {
				result = TimeType{}
			}
			if lt == avro.TimestampMicros {
				result = TimestampType{}
			}
		}
	}

	if result == nil {
		return nil, fmt.Errorf("unsupported logical type: %v:%v", t, lt)
	}

	return result, nil
}

func (c *toIcebergConverter) convertArray(avsc *avro.ArraySchema) (Type, error) {
	var err error
	var elem Type
	var itemsc avro.Schema
	var required bool

	if avsc.Prop(LOGICAL_TYPE_PROP) == "map" {
		return c.convertLogicalMap(avsc)
	}

	if itemsc, required, err = resolveUnion(avsc.Items()); err != nil {
		return nil, err
	}

	if elem, err = c.convertSchema(itemsc); err != nil {
		return nil, err
	}

	return &ListType{
		ElementID:       c.getElementID(avsc),
		Element:         elem,
		ElementRequired: required,
	}, nil
}

func (c *toIcebergConverter) convertFixed(avsc *avro.FixedSchema) (Type, error) {
	return FixedType{
		len: avsc.Size(),
	}, nil

}

func (c *toIcebergConverter) convertField(field *avro.Field) (NestedField, error) {
	var err error
	var avsc avro.Schema
	var typ Type
	var required bool

	if avsc, required, err = resolveUnion(field.Type()); err != nil {
		return NestedField{}, err
	}

	if typ, err = c.convertSchema(avsc); err != nil {
		return NestedField{}, err
	}

	return NestedField{
		Type:         typ,
		ID:           c.getFieldID(field),
		Name:         field.Name(),
		Required:     required,
		Doc:          field.Doc(),
		WriteDefault: field.Default(),
	}, nil
}

func (c *toIcebergConverter) convertLogicalMap(avsc *avro.ArraySchema) (Type, error) {
	var err error
	var key NestedField
	var value NestedField

	if avsc.Items().Type() != avro.Record {
		return nil, fmt.Errorf("unsupported logical map type: %v", avsc.Items().Type())
	}

	fields := avsc.Items().(*avro.RecordSchema).Fields()
	if len(fields) != 2 {
		return nil, fmt.Errorf("logical map must have 2 fields, but has %d", len(fields))
	}

	if key, err = c.convertField(fields[0]); err != nil {
		return nil, err
	}

	if value, err = c.convertField(fields[1]); err != nil {
		return nil, err
	}

	return &MapType{
		KeyID:         key.ID,
		KeyType:       key.Type,
		ValueID:       value.ID,
		ValueType:     value.Type,
		ValueRequired: value.Required,
	}, nil
}

func (c *toIcebergConverter) convertMap(avsc *avro.MapSchema) (Type, error) {
	var err error
	var valueType Type
	var valuesc avro.Schema
	var required bool

	if valuesc, required, err = resolveUnion(avsc.Values()); err != nil {
		return nil, err
	}

	if valueType, err = c.convertSchema(valuesc); err != nil {
		return nil, err
	}

	return &MapType{
		KeyID:         c.getKeyID(avsc),
		KeyType:       StringType{},
		ValueID:       c.getValueID(avsc),
		ValueType:     valueType,
		ValueRequired: required,
	}, nil
}

func (c *toIcebergConverter) convertRecord(avsc *avro.RecordSchema) (Type, error) {
	var err error

	fields := make([]NestedField, len(avsc.Fields()))
	for i, field := range avsc.Fields() {
		if fields[i], err = c.convertField(field); err != nil {
			return nil, err
		}
	}

	return &StructType{
		FieldList: fields,
	}, nil
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
