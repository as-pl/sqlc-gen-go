package golang

import (
	"testing"

	"github.com/sqlc-dev/plugin-sdk-go/metadata"
	"github.com/sqlc-dev/plugin-sdk-go/plugin"
	"github.com/sqlc-dev/sqlc-gen-go/internal/opts"
)

func testOptions() *opts.Options {
	return &opts.Options{
		EmitDbTags:          true,
		EmitJsonTags:        false,
		EmitSqlAsComment:    false,
		EmitExportedQueries: true,
		InitialismsMap:      map[string]struct{}{"id": {}},
		QueryParameterLimit: func() *int32 { v := int32(1); return &v }(),
	}
}

func TestBuildStructs_TableStructEmitsRealName(t *testing.T) {
	options := &opts.Options{
		EmitDbTags:          true,
		EmitJsonTags:        false,
		InitialismsMap:      map[string]struct{}{"id": {}},
		QueryParameterLimit: func() *int32 { v := int32(1); return &v }(),
	}

	req := &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "mysql"},
		Catalog: &plugin.Catalog{
			DefaultSchema: "as_db_atlas",
			Schemas: []*plugin.Schema{
				{
					Name: "as_db_atlas",
					Tables: []*plugin.Table{
						{
							Rel: &plugin.Identifier{Schema: "as_db_atlas", Name: "comm_mailer_queue"},
							Columns: []*plugin.Column{
								{
									Name:    "id",
									NotNull: true,
									Table:   &plugin.Identifier{Schema: "as_db_atlas", Name: "comm_mailer_queue"},
									Type:    &plugin.Identifier{Name: "int"},
								},
							},
						},
					},
				},
			},
		},
	}

	structs := buildStructs(req, options)
	if len(structs) != 1 {
		t.Fatalf("expected 1 struct, got %d", len(structs))
	}
	if len(structs[0].Fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(structs[0].Fields))
	}
	tags := structs[0].Fields[0].Tags
	if got := tags["real_name"]; got != "id" {
		t.Fatalf("expected real_name=id, got %q", got)
	}
	if _, ok := tags["col_real_name"]; ok {
		t.Fatalf("expected col_real_name to be absent")
	}
}

func TestBuildQueries_DynamicDoesNotReuseModelStruct(t *testing.T) {
	options := testOptions()

	tableID := &plugin.Identifier{Schema: "as_db_atlas", Name: "comm_mailer_queue"}
	model := Struct{
		Table: tableID,
		Name:  "CommMailerQueue",
		Fields: []Field{
			{Name: "ID", Type: "int32"},
			{Name: "Email", Type: "string"},
		},
	}

	q := &plugin.Query{
		Name: "GetEmails",
		Cmd:  metadata.CmdMany,
		// The generator's alias parser relies on query text (not Columns metadata) for table aliases.
		Text: "SELECT m.id, m.email FROM comm_mailer_queue m",
		Columns: []*plugin.Column{
			{
				Name:         "id",
				OriginalName: "id",
				NotNull:      true,
				Table:        tableID,
				Type:         &plugin.Identifier{Name: "int"},
			},
			{
				Name:         "email",
				OriginalName: "email",
				NotNull:      true,
				Table:        tableID,
				Type:         &plugin.Identifier{Name: "varchar"},
			},
		},
		Comments: []string{"dynamic"},
	}

	req := &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "mysql"},
		Catalog:  &plugin.Catalog{DefaultSchema: "as_db_atlas"},
		Queries:  []*plugin.Query{q},
	}

	queries, err := buildQueries(req, options, []Struct{model})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("expected 1 query, got %d", len(queries))
	}
	if queries[0].Ret.Struct == nil {
		t.Fatalf("expected Ret.Struct to be set")
	}
	if queries[0].Ret.Struct.Name != "GetEmailsRow" {
		t.Fatalf("expected dynamic query to emit Row struct, got %q", queries[0].Ret.Struct.Name)
	}
	if got := queries[0].Ret.Struct.Fields[0].Tags["real_name"]; got != "m.id" {
		t.Fatalf("expected real_name m.id, got %q", got)
	}
	if got := queries[0].Ret.Struct.Fields[1].Tags["real_name"]; got != "m.email" {
		t.Fatalf("expected real_name m.email, got %q", got)
	}
}

func TestBuildQueries_DynamicMarksRequiredJoins(t *testing.T) {
	options := testOptions()

	q := &plugin.Query{
		Name: "GetHistory",
		Cmd:  metadata.CmdMany,
		Text: "SELECT d.id, u.login, sp.name FROM erp_quality_pz_dimmensions d JOIN access_user u ON d.user_id = u.id JOIN shop_properties sp ON d.property_id = sp.id WHERE d.product_name = ?",
		Columns: []*plugin.Column{
			{
				Name:         "id",
				OriginalName: "id",
				NotNull:      true,
				Table:        &plugin.Identifier{Schema: "as_db_atlas", Name: "erp_quality_pz_dimmensions"},
				Type:         &plugin.Identifier{Name: "int"},
			},
		},
		Comments: []string{"dynamic", "required-joins: u, sp"},
	}

	req := &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "mysql"},
		Catalog:  &plugin.Catalog{DefaultSchema: "as_db_atlas"},
		Queries:  []*plugin.Query{q},
	}

	queries, err := buildQueries(req, options, nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	joins := queries[0].QueryToCountParts.Joins
	if !joins["u"].Required {
		t.Fatalf("expected join u to be required")
	}
	if !joins["sp"].Required {
		t.Fatalf("expected join sp to be required")
	}
}

func TestBuildQueries_DynamicRejectsUnknownRequiredJoin(t *testing.T) {
	options := testOptions()

	q := &plugin.Query{
		Name:     "GetHistory",
		Cmd:      metadata.CmdMany,
		Text:     "SELECT d.id FROM erp_quality_pz_dimmensions d JOIN access_user u ON d.user_id = u.id",
		Columns:  []*plugin.Column{},
		Comments: []string{"dynamic", "required-joins: missing"},
	}

	req := &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "mysql"},
		Catalog:  &plugin.Catalog{DefaultSchema: "as_db_atlas"},
		Queries:  []*plugin.Query{q},
	}

	if _, err := buildQueries(req, options, nil); err == nil {
		t.Fatalf("expected error for unknown required join alias")
	}
}
