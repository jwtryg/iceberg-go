package iceberg_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
)

var avscManifestFile = avro.MustParse(`{
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

var icebscgManifestFile = iceberg.NewSchema(0, []iceberg.NestedField{
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
	expected := icebscgManifestFile
	actual, err := iceberg.AvroToIceberg(avscManifestFile)
	assert.NoError(t, err)
	assert.Equal(t, expected.ID, actual.ID)
	assert.Equal(t, expected.Fields(), actual.Fields())
}

func TestIcebergManifestToAvro(t *testing.T) {
	expected := avscManifestFile
	actual, err := iceberg.IcebergToAvro("manifest_file", icebscgManifestFile)
	assert.NoError(t, err)
	assert.Equal(t, expected.String(), actual.String())
}
