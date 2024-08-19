package iceberg_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
)

var avroManifestFile = avro.MustParse(`{
	"type": "record",
	"name": "manifest_file",
	"fields": [
		{"name": "manifest_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 500},
		{"name": "manifest_length", "type": "long", "doc": "Total file size in bytes", "field-id": 501},
		{"name": "partition_spec_id", "type": "int", "doc": "Spec ID used to write", "field-id": 502},
		{"name": "content", "type": "int", "doc": "Contents of the manifest: 0=data, 1=deletes", "field-id": 517},
		{
			"name": "sequence_number",
			"type": "long",
			"doc": "Sequence number when the manifest was added",
			"field-id": 515
		},
		{
			"name": "min_sequence_number",
			"type": "long",
			"doc": "Lowest sequence number in the manifest",
			"field-id": 516
		},
		{"name": "added_snapshot_id", "type": "long", "doc": "Snapshot ID that added the manifest", "field-id": 503},
		{"name": "added_files_count", "type": "int", "doc": "Added entry count", "field-id": 504},
		{"name": "existing_files_count", "type": "int", "doc": "Existing entry count", "field-id": 505},
		{"name": "deleted_files_count", "type": "int", "doc": "Deleted entry count", "field-id": 506},
		{"name": "added_rows_count", "type": "long", "doc": "Added rows count", "field-id": 512},
		{"name": "existing_rows_count", "type": "long", "doc": "Existing rows count", "field-id": 513},
		{"name": "deleted_rows_count", "type": "long", "doc": "Deleted rows count", "field-id": 514},
		{
			"name": "partitions",
			"type": [
				"null",
				{
					"type": "array",
					"items": {
						"type": "record",
						"name": "r508",
						"fields": [
							{
								"name": "contains_null",
								"type": "boolean",
								"doc": "True if any file has a null partition value",
								"field-id": 509
							},
							{
								"name": "contains_nan",
								"type": ["null", "boolean"],
								"doc": "True if any file has a nan partition value",                                  
								"field-id": 518
							},
							{
								"name": "lower_bound",
								"type": ["null", "bytes"],
								"doc": "Partition lower bound for all files",                                    
								"field-id": 510
							},
							{
								"name": "upper_bound",
								"type": ["null", "bytes"],
								"doc": "Partition upper bound for all files",
								"field-id": 511
							}
						]
					},
					"element-id": 508
				}
			],
			"doc": "Summary for each partition",
			"field-id": 507
		}
	]
}`)

var icebergManifestFile = iceberg.NewSchema(0, []iceberg.NestedField{
	{ID: 500, Name: "manifest_path", Type: iceberg.StringType{}, Required: true, Doc: "Location URI with FS scheme"},
	{ID: 501, Name: "manifest_length", Type: iceberg.Int64Type{}, Required: true, Doc: "Total file size in bytes"},
	{ID: 502, Name: "partition_spec_id", Type: iceberg.Int32Type{}, Required: true, Doc: "Spec ID used to write"},
	{ID: 517, Name: "content", Type: iceberg.Int32Type{}, Required: true, Doc: "Contents of the manifest: 0=data, 1=deletes"},
	{
		ID:       515,
		Name:     "sequence_number",
		Type:     iceberg.Int64Type{},
		Required: true,
		Doc:      "Sequence number when the manifest was added",
	},
	{
		ID:       516,
		Name:     "min_sequence_number",
		Type:     iceberg.Int64Type{},
		Required: true,
		Doc:      "Lowest sequence number in the manifest",
	},
	{ID: 503, Name: "added_snapshot_id", Type: iceberg.Int64Type{}, Required: true, Doc: "Snapshot ID that added the manifest"},
	{ID: 504, Name: "added_files_count", Type: iceberg.Int32Type{}, Required: true, Doc: "Added entry count"},
	{ID: 505, Name: "existing_files_count", Type: iceberg.Int32Type{}, Required: true, Doc: "Existing entry count"},
	{ID: 506, Name: "deleted_files_count", Type: iceberg.Int32Type{}, Required: true, Doc: "Deleted entry count"},
	{ID: 512, Name: "added_rows_count", Type: iceberg.Int64Type{}, Required: true, Doc: "Added rows count"},
	{ID: 513, Name: "existing_rows_count", Type: iceberg.Int64Type{}, Required: true, Doc: "Existing rows count"},
	{ID: 514, Name: "deleted_rows_count", Type: iceberg.Int64Type{}, Required: true, Doc: "Deleted rows count"},
	{
		ID:   507,
		Name: "partitions",
		Type: &iceberg.ListType{
			ElementID: 508,
			Element: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{
						ID:       509,
						Name:     "contains_null",
						Type:     iceberg.BooleanType{},
						Required: true,
						Doc:      "True if any file has a null partition value",
					},
					{
						ID:       518,
						Name:     "contains_nan",
						Type:     iceberg.BooleanType{},
						Required: false,
						Doc:      "True if any file has a nan partition value",
					},
					{
						ID:       510,
						Name:     "lower_bound",
						Type:     iceberg.BinaryType{},
						Required: false,
						Doc:      "Partition lower bound for all files",
					},
					{
						ID:       511,
						Name:     "upper_bound",
						Type:     iceberg.BinaryType{},
						Required: false,
						Doc:      "Partition upper bound for all files",
					},
				},
			},
			ElementRequired: true,
		},
		Required: false,
		Doc:      "Summary for each partition",
	},
}...)

func TestAvroManifestToIceberg(t *testing.T) {
	expected := icebergManifestFile
	actual, err := iceberg.AvroToIceberg(avroManifestFile)
	assert.NoError(t, err)
	assert.Equal(t, expected.ID, actual.ID)
	assert.Equal(t, expected.Fields(), actual.Fields())
}

func TestIcebergManifestToAvro(t *testing.T) {
	expected := avroManifestFile
	actual, err := iceberg.IcebergToAvro("manifest_file", icebergManifestFile)
	assert.NoError(t, err)
	assert.Equal(t, expected.String(), actual.String())
}

// Assign IDs to all fields in the Avro schema
// in post-order traversal of the schema tree.
// Further, notice how record names are assigned
// as "r" followed by the field-id of the record.
func TestAvroAssignIDs(t *testing.T) {
	avroSchema := avro.MustParse(`{
		"type": "record",
		"name": "avro_schema",
		"fields": [
			{"name": "field1", "type": "string"},
			{
				"name": "field2", 
				"type": {
					"type": "record",
					"name": "record_1",
					"fields": [
						{"name": "nested_field1", "type": "int"},
						{
							"name": "nested_field2", 
							"type":  {
								"type": "array",
								"items": "long"
							}
						},
						{
							"name": "nested_field3", 
							"type": {
								"type": "map",
								"values": "string"
							}
						}
					]	
				}
			},
			{"name": "field3", "type": "long"}
		]
	}`)

	expected := avro.MustParse(`{
		"type": "record",
		"name": "avro_schema",
		"fields": [
			{"name": "field1", "type": "string", "field-id": 0},
			{
				"name": "field2", 
				"type": {
					"type": "record",
					"name": "r7",
					"fields": [
						{"name": "nested_field1", "type": "int", "field-id": 1},
						{
							"name": "nested_field2", 
							"type":  {
								"type": "array",
								"items": "long",
								"element-id": 2
							},
							"field-id": 3
						},
						{
							"name": "nested_field3", 
							"type": {
								"type": "map",
								"values": "string",
								"key-id": 4, 
								"value-id": 5
							},
							"field-id": 6
						}
					]
				},
				"field-id": 7
			},
			{"name": "field3", "type": "long", "field-id": 8}
		]
	}`)

	icebergSchema, err := iceberg.AvroToIceberg(avroSchema)
	assert.NoError(t, err)

	idx, err := iceberg.IndexByName(icebergSchema)
	assert.NoError(t, err)
	_ = idx

	actual, err := iceberg.IcebergToAvro("avro_schema", icebergSchema)
	assert.NoError(t, err)
	assert.Equal(t, expected.String(), actual.String())
}

func TestAvroListRequiredPrimitive(t *testing.T) {
	avroSchema := avro.MustParse(`{
        "type": "record",
        "name": "avro_schema",
        "fields": [{
			"name": "array_with_string",
			"type": {
				"type": "array",
				"items": "string",
				"default": [],
				"element-id": 101
			},
			"field-id": 100
		}]
    }`)

	expectedIceberg := iceberg.NewSchema(0, []iceberg.NestedField{{
		ID:       100,
		Name:     "array_with_string",
		Type:     &iceberg.ListType{ElementID: 101, Element: iceberg.StringType{}, ElementRequired: true},
		Required: true,
	}}...)

	actualIceberg, err := iceberg.AvroToIceberg(avroSchema)
	assert.NoError(t, err)
	assert.Equal(t, expectedIceberg.ID, actualIceberg.ID)
	assert.Equal(t, expectedIceberg.Fields(), actualIceberg.Fields())
}

func TestAvroListWrappedPrimitive(t *testing.T) {
	avroSchema := avro.MustParse(`{
        "type": "record",
        "name": "avro_schema",
        "fields": [{
			"name": "array_with_string",
			"type": {
				"type": "array",
				"items": {"type": "string"},
				"default": [],
				"element-id": 101
			},
			"field-id": 100
		}]
    }`)

	expectedIceberg := iceberg.NewSchema(0, []iceberg.NestedField{{
		ID:       100,
		Name:     "array_with_string",
		Type:     &iceberg.ListType{ElementID: 101, Element: iceberg.StringType{}, ElementRequired: true},
		Required: true,
	}}...)

	actualIceberg, err := iceberg.AvroToIceberg(avroSchema)
	assert.NoError(t, err)
	assert.Equal(t, expectedIceberg.ID, actualIceberg.ID)
	assert.Equal(t, expectedIceberg.Fields(), actualIceberg.Fields())
}

// func TestAvroLogicalMap(t *testing.T) {
// 	avroSchema := avro.MustParse(`{
//         "type": "record",
//         "name": "avro_schema",
//         "fields": [
//             {
//                 "name": "array_with_string",
//                 "type": {
//                     "type": "array",
// 					"logicalType": "map",
// 					"some-annotation": "some-value",
//                     "items": {
// 						"type": "record",
// 						"name": "k101_v102",
// 						"fields": [
// 							{"name": "key", "type": "int", "field-id": 101},
// 							{"name": "value", "type": "string", "field-id": 102}
// 						]
// 					},
//                     "element-id": 101
//                 },
//                 "field-id": 100
//             }
//         ]
//     }`)

// 	expectedIceberg := iceberg.NewSchema(0, []iceberg.NestedField{{
// 		ID:   100,
// 		Name: "array_with_string",
// 		Type: &iceberg.ListType{
// 			ElementID: 101,
// 			Element: &iceberg.MapType{
// 				KeyID: 101, KeyType: iceberg.Int32Type{},
// 				ValueID: 102, ValueType: iceberg.StringType{}, ValueRequired: true,
// 			},
// 			ElementRequired: true,
// 		},
// 	}}...)

// 	actualIceberg, err := iceberg.AvroToIceberg(avroSchema)
// 	assert.NoError(t, err)
// 	assert.Equal(t, expectedIceberg.ID, actualIceberg.ID)
// 	assert.Equal(t, expectedIceberg.Fields(), actualIceberg.Fields())
// }

func TestAvroInvalidUnion(t *testing.T) {
	avroSchema := avro.MustParse(`{
		"type": "record",
		"name": "avro_schema",
		"fields": [{
			"name": "union_with_null",
			"type": ["null", "string", "long"],
			"field-id": 100
		}]
	}`)
	_, err := iceberg.AvroToIceberg(avroSchema)
	assert.Error(t, err)
}

func TestAvroUnionWithDefault(t *testing.T) {
	avroSchema := avro.MustParse(`{
		"type": "record",
		"name": "avro_schema",
		"fields": [{
			"name": "union_with_null",
			"type": ["string", "null"],
			"default": "empty",
			"field-id": 100
		}]
	}`)
	expectedIceberg := iceberg.NewSchema(0, []iceberg.NestedField{{
		ID:           100,
		Name:         "union_with_null",
		Type:         iceberg.StringType{},
		Required:     false,
		WriteDefault: "empty",
	}}...)

	actualIceberg, err := iceberg.AvroToIceberg(avroSchema)
	assert.NoError(t, err)
	assert.Equal(t, expectedIceberg.ID, actualIceberg.ID)
	assert.Equal(t, expectedIceberg.Fields(), actualIceberg.Fields())
}

func TestAvroMapType(t *testing.T) {
	avroSchema := avro.MustParse(`{
		"type": "record",
		"name": "avro_schema",
		"fields": [{
			"name": "map",
			"type": {
				"type": "map",
				"values": ["null", "long"],
				"key-id": 101,
				"value-id": 102
			}
		}]
    }`)

	expectedIceberg := iceberg.NewSchema(0, []iceberg.NestedField{{
		ID:   0,
		Name: "map",
		Type: &iceberg.MapType{
			KeyID: 101, KeyType: iceberg.StringType{},
			ValueID: 102, ValueType: iceberg.Int64Type{}, ValueRequired: false,
		},
		Required: true,
	}}...)

	actualIceberg, err := iceberg.AvroToIceberg(avroSchema)
	assert.NoError(t, err)
	assert.Equal(t, expectedIceberg.ID, actualIceberg.ID)
	assert.Equal(t, expectedIceberg.Fields(), actualIceberg.Fields())
}

func TestAvroFixedType(t *testing.T) {
	avroSchema := avro.MustParse(`{
		"type": "record",
		"name": "avro_schema",
		"fields": [{
			"name": "field",
			"type": { "type": "fixed", "name": "fixed", "size": 22 }
		}]
	}`)

	expected := iceberg.NewSchema(0, []iceberg.NestedField{{
		ID:       0,
		Name:     "field",
		Type:     iceberg.FixedTypeOf(22),
		Required: true,
	}}...)

	actual, err := iceberg.AvroToIceberg(avroSchema)
	assert.NoError(t, err)
	assert.Equal(t, expected.ID, actual.ID)
	assert.Equal(t, expected.Fields(), actual.Fields())
}

func TestAvroConvertNonRecordSchema(t *testing.T) {
	avroSchema := avro.MustParse(`{
		"type": "array",
        "items": "string",
        "default": []
	}`)

	_, err := iceberg.AvroToIceberg(avroSchema)
	assert.ErrorContains(t, err, "schema must be a record")
}

func TestAvroConvertDecimalType(t *testing.T) {
	avroSchema := avro.MustParse(`{
		"type": "record",
		"name": "avro_schema",
		"fields": [{
			"name": "field",
			"type": {
				"type": "fixed", 
				"name": "fixed",
				"size": 16,
				"logicalType": "decimal", 
				"precision": 20, 
				"scale": 15
			}
		}]
	}`)

	expected := iceberg.NewSchema(0, []iceberg.NestedField{{
		ID:       0,
		Name:     "field",
		Type:     iceberg.DecimalTypeOf(20, 15),
		Required: true,
	}}...)

	actual, err := iceberg.AvroToIceberg(avroSchema)
	assert.NoError(t, err)
	assert.Equal(t, expected.Fields(), actual.Fields())
}
