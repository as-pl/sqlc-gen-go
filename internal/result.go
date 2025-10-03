package golang

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/sqlc-dev/plugin-sdk-go/metadata"
	"github.com/sqlc-dev/plugin-sdk-go/plugin"
	"github.com/sqlc-dev/plugin-sdk-go/sdk"
	"github.com/sqlc-dev/sqlc-gen-go/internal/inflection"
	"github.com/sqlc-dev/sqlc-gen-go/internal/opts"
	"github.com/xwb1989/sqlparser"
)

func buildEnums(req *plugin.GenerateRequest, options *opts.Options) []Enum {
	var enums []Enum
	for _, schema := range req.Catalog.Schemas {
		if schema.Name == "pg_catalog" || schema.Name == "information_schema" {
			continue
		}
		for _, enum := range schema.Enums {
			var enumName string
			if schema.Name == req.Catalog.DefaultSchema {
				enumName = enum.Name
			} else {
				enumName = schema.Name + "_" + enum.Name
			}

			e := Enum{
				Name:      StructName(enumName, options),
				Comment:   enum.Comment,
				NameTags:  map[string]string{},
				ValidTags: map[string]string{},
			}
			if options.EmitJsonTags {
				e.NameTags["json"] = JSONTagName(enumName, options)
				e.ValidTags["json"] = JSONTagName("valid", options)
			}

			seen := make(map[string]struct{}, len(enum.Vals))
			for i, v := range enum.Vals {
				value := EnumReplace(v)
				if _, found := seen[value]; found || value == "" {
					value = fmt.Sprintf("value_%d", i)
				}
				e.Constants = append(e.Constants, Constant{
					Name:  StructName(enumName+"_"+value, options),
					Value: v,
					Type:  e.Name,
				})
				seen[value] = struct{}{}
			}
			enums = append(enums, e)
		}
	}
	if len(enums) > 0 {
		sort.Slice(enums, func(i, j int) bool { return enums[i].Name < enums[j].Name })
	}
	return enums
}

func buildStructs(req *plugin.GenerateRequest, options *opts.Options) []Struct {
	var structs []Struct
	for _, schema := range req.Catalog.Schemas {
		if schema.Name == "pg_catalog" || schema.Name == "information_schema" {
			continue
		}
		for _, table := range schema.Tables {
			var tableName string
			if schema.Name == req.Catalog.DefaultSchema {
				tableName = table.Rel.Name
			} else {
				tableName = schema.Name + "_" + table.Rel.Name
			}
			structName := tableName
			if !options.EmitExactTableNames {
				structName = inflection.Singular(inflection.SingularParams{
					Name:       structName,
					Exclusions: options.InflectionExcludeTableNames,
				})
			}
			s := Struct{
				Table:   &plugin.Identifier{Schema: schema.Name, Name: table.Rel.Name},
				Name:    StructName(structName, options),
				Comment: table.Comment,
			}
			for _, column := range table.Columns {
				tags := map[string]string{}
				if options.EmitDbTags {
					tags["db"] = column.Name
				}
				if options.EmitJsonTags {
					tags["json"] = JSONTagName(column.Name, options)
				}
				tags["col_real_name"] = "table." + column.Name + "." + StructName(column.Name, options)
				addExtraGoStructTags(tags, req, options, column)
				s.Fields = append(s.Fields, Field{
					Name:    StructName(column.Name, options),
					Type:    goType(req, options, column),
					Tags:    tags,
					Comment: column.Comment,
				})
			}
			structs = append(structs, s)
		}
	}
	if len(structs) > 0 {
		sort.Slice(structs, func(i, j int) bool { return structs[i].Name < structs[j].Name })
	}
	return structs
}

type goColumn struct {
	id int
	*plugin.Column
	embed *goEmbed
}

type goEmbed struct {
	modelType string
	modelName string
	fields    []Field
}

// look through all the structs and attempt to find a matching one to embed
// We need the name of the struct and its field names.
func newGoEmbed(embed *plugin.Identifier, structs []Struct, defaultSchema string) *goEmbed {
	if embed == nil {
		return nil
	}

	for _, s := range structs {
		embedSchema := defaultSchema
		if embed.Schema != "" {
			embedSchema = embed.Schema
		}

		// compare the other attributes
		if embed.Catalog != s.Table.Catalog || embed.Name != s.Table.Name || embedSchema != s.Table.Schema {
			continue
		}

		fields := make([]Field, len(s.Fields))
		for i, f := range s.Fields {
			fields[i] = f
		}

		return &goEmbed{
			modelType: s.Name,
			modelName: s.Name,
			fields:    fields,
		}
	}

	return nil
}

func columnName(c *plugin.Column, pos int) string {
	if c.Name != "" {
		return c.Name
	}
	return fmt.Sprintf("column_%d", pos+1)
}

func paramName(p *plugin.Parameter) string {
	if p.Column.Name != "" {
		return argName(p.Column.Name)
	}
	return fmt.Sprintf("dollar_%d", p.Number)
}

func argName(name string) string {
	out := ""
	for i, p := range strings.Split(name, "_") {
		if i == 0 {
			out += strings.ToLower(p)
		} else if p == "id" {
			out += "ID"
		} else {
			out += strings.Title(p)
		}
	}
	return out
}

func buildQueries(req *plugin.GenerateRequest, options *opts.Options, structs []Struct) ([]Query, error) {
	qs := make([]Query, 0, len(req.Queries))
	for _, query := range req.Queries {
		if query.Name == "" {
			continue
		}
		if query.Cmd == "" {
			continue
		}

		var constantName string
		if options.EmitExportedQueries {
			constantName = sdk.Title(query.Name)
		} else {
			constantName = sdk.LowerTitle(query.Name)
		}

		comments := query.Comments
		if options.EmitSqlAsComment {
			if len(comments) == 0 {
				comments = append(comments, query.Name)
			}
			comments = append(comments, " ")
			scanner := bufio.NewScanner(strings.NewReader(query.Text))
			for scanner.Scan() {
				line := scanner.Text()
				comments = append(comments, "  "+line)
			}
			if err := scanner.Err(); err != nil {
				return nil, err
			}
		}

		commentsMap := make(map[string]string)
		for _, c := range comments {
			tmp := strings.SplitN(c, ":", 2)
			if len(tmp) == 2 {
				commentsMap[strings.TrimSpace(tmp[0])] = strings.TrimSpace(tmp[1])
			} else {
				commentsMap[strings.TrimSpace(tmp[0])] = ""
			}
		}
		for k, v := range commentsMap {
			mylog(fmt.Sprintf("Comment '%s': %s", k, v))
		}

		_, isDynamic := commentsMap["dynamic"]

		mylog(fmt.Sprintf(" is dynamic %s '%v'", query.Name, isDynamic))

		parsed := QueryLightAST{}

		if isDynamic {
			var err error
			parsed, err = ParseQueryToCountParts(query.Text)
			if err != nil {
				return nil, err
			}
			parsed.Meta = commentsMap
		}

		gq := Query{ //12311112223222
			Cmd:               query.Cmd,
			ConstantName:      constantName,
			FieldName:         sdk.LowerTitle(query.Name) + "Stmt",
			MethodName:        query.Name,
			SourceName:        query.Filename,
			SQL:               query.Text,
			Comments:          comments,
			Table:             query.InsertIntoTable,
			IsDynamic:         isDynamic,
			QueryToCountParts: parsed,
		}
		sqlpkg := parseDriver(options.SqlPackage)

		qpl := int(*options.QueryParameterLimit)

		if len(query.Params) == 1 && qpl != 0 {
			p := query.Params[0]
			gq.Arg = QueryValue{
				Name:      escape(paramName(p)),
				DBName:    p.Column.GetName(),
				Typ:       goType(req, options, p.Column),
				SQLDriver: sqlpkg,
				Column:    p.Column,
			}
		} else if len(query.Params) >= 1 {
			var cols []goColumn
			for _, p := range query.Params {
				cols = append(cols, goColumn{
					id:     int(p.Number),
					Column: p.Column,
				})
			}
			s, err := columnsToStruct(req, query, options, gq.MethodName+"Params", cols, false)
			if err != nil {
				return nil, err
			}
			gq.Arg = QueryValue{
				Emit:        true,
				Name:        "arg",
				Struct:      s,
				SQLDriver:   sqlpkg,
				EmitPointer: options.EmitParamsStructPointers,
			}

			// if query params is 2, and query params limit is 4 AND this is a copyfrom, we still want to emit the query's model
			// otherwise we end up with a copyfrom using a struct without the struct definition
			if len(query.Params) <= qpl && query.Cmd != ":copyfrom" {
				gq.Arg.Emit = false
			}
		}

		if len(query.Columns) == 1 && query.Columns[0].EmbedTable == nil {
			c := query.Columns[0]
			name := columnName(c, 0)
			name = strings.Replace(name, "$", "_", -1)
			gq.Ret = QueryValue{
				Name:      escape(name),
				DBName:    name,
				Typ:       goType(req, options, c),
				SQLDriver: sqlpkg,
			}
		} else if putOutColumns(query) {
			var gs *Struct
			var emit bool

			for _, s := range structs {
				if len(s.Fields) != len(query.Columns) {
					continue
				}
				same := true
				for i, f := range s.Fields {
					c := query.Columns[i]
					sameName := f.Name == StructName(columnName(c, i), options)
					sameType := f.Type == goType(req, options, c)
					sameTable := sdk.SameTableName(c.Table, s.Table, req.Catalog.DefaultSchema)
					if !sameName || !sameType || !sameTable {
						same = false
					}
				}
				if same {
					gs = &s
					break
				}
			}

			if gs == nil {
				var columns []goColumn
				for i, c := range query.Columns {
					columns = append(columns, goColumn{
						id:     i,
						Column: c,
						embed:  newGoEmbed(c.EmbedTable, structs, req.Catalog.DefaultSchema),
					})
				}
				var err error
				gs, err = columnsToStruct(req, query, options, gq.MethodName+"Row", columns, true)
				if err != nil {
					return nil, err
				}
				emit = true
			}
			gq.Ret = QueryValue{
				Emit:        emit,
				Name:        "i",
				Struct:      gs,
				SQLDriver:   sqlpkg,
				EmitPointer: options.EmitResultStructPointers,
			}
		}

		qs = append(qs, gq)
	}
	sort.Slice(qs, func(i, j int) bool { return qs[i].MethodName < qs[j].MethodName })
	return qs, nil
}

var cmdReturnsData = map[string]struct{}{
	metadata.CmdBatchMany: {},
	metadata.CmdBatchOne:  {},
	metadata.CmdMany:      {},
	metadata.CmdOne:       {},
}

func putOutColumns(query *plugin.Query) bool {
	_, found := cmdReturnsData[query.Cmd]
	return found
}

// It's possible that this method will generate duplicate JSON tag values
//
//	Columns: count, count,   count_2
//	 Fields: Count, Count_2, Count2
//
// JSON tags: count, count_2, count_2
//
// This is unlikely to happen, so don't fix it yet
func columnsToStruct(req *plugin.GenerateRequest, query *plugin.Query, options *opts.Options, name string, columns []goColumn, useID bool) (*Struct, error) {
	gs := Struct{
		Name: name,
	}
	seen := map[string][]int{}
	suffixes := map[int]int{}

	columnAliasSchema, err2 := parse(query.GetText())
	if err2 != nil {
		//return nil, err2
		//do nothing, we don't have alias info
		//fmt.Println("Warning: cannot parse query for column aliases:", err2)
	}
	for i, c := range columns {
		colName := columnName(c.Column, i)
		tagName := colName

		// override col/tag with expected model name
		if c.embed != nil {
			colName = c.embed.modelName
			tagName = SetCaseStyle(colName, "snake")
		}

		fieldName := StructName(colName, options)
		baseFieldName := fieldName
		// Track suffixes by the ID of the column, so that columns referring to the same numbered parameter can be
		// reused.
		suffix := 0
		if o, ok := suffixes[c.id]; ok && useID {
			suffix = o
		} else if v := len(seen[fieldName]); v > 0 && !c.IsNamedParam {
			suffix = v + 1
		}
		suffixes[c.id] = suffix
		if suffix > 0 {
			tagName = fmt.Sprintf("%s_%d", tagName, suffix)
			fieldName = fmt.Sprintf("%s_%d", fieldName, suffix)
		}
		tags := map[string]string{}
		if options.EmitDbTags {
			tags["db"] = tagName
		}
		if options.EmitJsonTags {
			tags["json"] = JSONTagName(tagName, options)
		}

		//fmt.Println(c)
		alias := ""

		for _, col := range columnAliasSchema {
			if col.ColumnName == c.GetOriginalName() && (col.ColumnAlias == "" || col.ColumnAlias == c.GetName()) && col.TableAlias != "" {
				alias = col.TableAlias
			}
		}

		tags["real_name"] = c.GetOriginalName()
		if alias != "" {
			tags["real_name"] = alias + "." + c.GetOriginalName()
		}

		addExtraGoStructTags(tags, req, options, c.Column)
		f := Field{
			Name:   fieldName,
			DBName: colName,
			Tags:   tags,
			Column: c.Column,
		}
		if c.embed == nil {
			f.Type = goType(req, options, c.Column)
		} else {
			f.Type = c.embed.modelType
			f.EmbedFields = c.embed.fields
		}

		gs.Fields = append(gs.Fields, f)
		if _, found := seen[baseFieldName]; !found {
			seen[baseFieldName] = []int{i}
		} else {
			seen[baseFieldName] = append(seen[baseFieldName], i)
		}
	}

	// If a field does not have a known type, but another
	// field with the same name has a known type, assign
	// the known type to the field without a known type
	for i, field := range gs.Fields {
		if len(seen[field.Name]) > 1 && field.Type == "interface{}" {
			for _, j := range seen[field.Name] {
				if i == j {
					continue
				}
				otherField := gs.Fields[j]
				if otherField.Type != field.Type {
					field.Type = otherField.Type
				}
				gs.Fields[i] = field
			}
		}
	}

	err := checkIncompatibleFieldTypes(gs.Fields)
	if err != nil {
		return nil, err
	}

	return &gs, nil
}

func checkIncompatibleFieldTypes(fields []Field) error {
	fieldTypes := map[string]string{}
	for _, field := range fields {
		if fieldType, found := fieldTypes[field.Name]; !found {
			fieldTypes[field.Name] = field.Type
		} else if field.Type != fieldType {
			return fmt.Errorf("named param %s has incompatible types: %s, %s", field.Name, field.Type, fieldType)
		}
	}
	return nil
}

func parse(query string) ([]ColumnMapping, error) {

	stmtRaw, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}

	stmt, ok := stmtRaw.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("not a SELECT statement")
	}

	// tables, err := extractTableAliases(stmt)
	// if err != nil {
	// 	return nil, err
	// }
	// f, err := os.Create("/home/artur/projects/as-pl-monorepo/tmp-gen-go/tmp.txt")
	// if err != nil {
	// 	return nil, err
	// }
	// defer f.Close()

	//w := bufio.NewWriter(f)
	//fmt.Fprintln(w, "Table aliases:")
	// for _, t := range tables {
	// 	fmt.Fprintf(w, "Alias: %s  → Table: %s\n", t.Alias, t.TableName)
	// }

	cols, err := extractColumnMappings(stmt)
	if err != nil {
		panic(err)
	}
	//fmt.Fprintln(w, "Columns:")
	// for _, c := range cols {
	// 	fmt.Fprintf(w, "ColumnName: %s | ColumnAlias: %s | TableAlias: %s\n",
	// 		c.ColumnName, c.ColumnAlias, c.TableAlias)
	// }
	//w.Flush()

	return cols, nil
}

type TableAlias struct {
	Alias     string // aliasu tabeli, lub nazwa tabeli jeśli alias nie podany
	TableName string // faktyczna nazwa tabeli
}

type ColumnMapping struct {
	ColumnName  string // nazwa kolumny, np. "company" albo "id"
	ColumnAlias string // alias kolumny, jeśli `AS` użyte np. `... AS ca`
	TableAlias  string // alias tabeli — np. "o", "alias_as_hell"
}

// Funkcja która mapuje kolumny z selekta do aliasów tabel
func extractColumnMappings(stmt *sqlparser.Select) ([]ColumnMapping, error) {
	var cols []ColumnMapping

	for _, selExpr := range stmt.SelectExprs {
		switch e := selExpr.(type) {
		case *sqlparser.StarExpr:
			// np. `*` albo `alias.*`
			tableAlias := e.TableName.Name.String()
			// jeśli jest alias przed gwiazdką
			cols = append(cols, ColumnMapping{
				ColumnName:  "*",
				ColumnAlias: "",
				TableAlias:  tableAlias,
			})
		case *sqlparser.AliasedExpr:
			// np. coś jak `alias.company AS customer_name` albo `company AS customer_name`
			var colAlias string
			if !e.As.IsEmpty() {
				colAlias = e.As.String()
			}
			// expr może być kolumną, funkcją lub innym wyrażeniem
			// podstawowy przypadek: `*sqlparser.ColName`
			switch col := e.Expr.(type) {
			case *sqlparser.ColName:
				// kolumna z table qualifier
				qualifier := col.Qualifier.Name.String() // alias tabeli lub pusta
				name := col.Name.String()
				cols = append(cols, ColumnMapping{
					ColumnName:  name,
					ColumnAlias: colAlias,
					TableAlias:  qualifier,
				})
			default:
				// inne wyrażenie, np. funkcja, literał, wyrażenie arytmetyczne
				// można potraktować podobnie: nie ma tableQualifier albo pusta
				cols = append(cols, ColumnMapping{
					ColumnName:  sqlparser.String(e.Expr),
					ColumnAlias: colAlias,
					TableAlias:  "",
				})
			}
		default:
			// inny typ SelectExpr? rzadziej się zdarza
			cols = append(cols, ColumnMapping{
				ColumnName:  sqlparser.String(selExpr),
				ColumnAlias: "",
				TableAlias:  "",
			})
		}
	}

	return cols, nil
}

// ParseQueryToCountParts czyści i parsuje SQL do części: FROM / JOINS / WHERE.
func ParseQueryToCountParts(raw string) (QueryLightAST, error) {
	sql := sanitizeSQL(raw)

	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return QueryLightAST{}, fmt.Errorf("sql parse error: %w", err)
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return QueryLightAST{}, fmt.Errorf("expected SELECT statement")
	}

	out := QueryLightAST{Joins: make(map[string]JoinPart)}

	// SELECT
	for _, expr := range sel.SelectExprs {
		switch e := expr.(type) {
		case *sqlparser.AliasedExpr:
			if e.As.String() != "" {
				out.Select = append(out.Select, sqlparser.String(e.Expr)+" AS "+e.As.String())
			} else {
				out.Select = append(out.Select, sqlparser.String(e.Expr))
			}
		case *sqlparser.StarExpr:
			if e.TableName.Name.String() != "" {
				out.Select = append(out.Select, e.TableName.Name.String()+".*")
			} else {
				out.Select = append(out.Select, "*")
			}
		default:
			out.Select = append(out.Select, sqlparser.String(expr))
		}
	}

	// FROM
	if len(sel.From) > 0 {
		if base := leftMostAliased(sel.From[0]); base != nil {
			out.From = sqlparser.String(base)
		} else {
			out.From = sqlparser.String(sel.From[0])
		}
	}

	// WHERE
	if sel.Where != nil && sel.Where.Expr != nil {
		out.Where = sqlparser.String(sel.Where.Expr)
		out.WhereCount = countPlaceholders(out.Where)
	}

	// JOINS
	for _, te := range sel.From {
		collectJoins(te, out.Joins)
	}

	// GROUP BY
	if len(sel.GroupBy) > 0 {
		raw = sqlparser.String(sel.GroupBy)
		out.GroupBy = trimPrefixFold(raw, "group by")
		out.GroupByCount = countPlaceholders(out.GroupBy)
	}

	// HAVING
	if sel.Having != nil && sel.Having.Expr != nil {
		raw = sqlparser.String(sel.Having.Expr)
		out.Having = trimPrefixFold(raw, "having")
		out.HavingCount = countPlaceholders(out.Having)
	}

	// ORDER BY
	if len(sel.OrderBy) > 0 {
		raw = sqlparser.String(sel.OrderBy)
		out.OrderBy = trimPrefixFold(raw, "order by")
		out.OrderByCount = countPlaceholders(out.OrderBy)
	}

	// LIMIT
	if sel.Limit != nil {
		if sel.Limit.Rowcount != nil {
			out.Limit = sqlparser.String(sel.Limit.Rowcount)
			out.LimitCount = countPlaceholders(out.Limit)
		}
		if sel.Limit.Offset != nil {
			out.Offset = sqlparser.String(sel.Limit.Offset)
			out.OffsetCount = countPlaceholders(out.Offset)
		}
	}

	return out, nil
}

func trimPrefixFold(s, prefix string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(strings.ToLower(s), strings.ToLower(prefix)) {
		return strings.TrimSpace(s[len(prefix):])
	}
	return s
}

// --- helpers ---

func countPlaceholders(fragment string) int {
	return strings.Count(fragment, "?")
}
func sanitizeSQL(s string) string {
	// usuń /* ... */ (blokowe)
	s = regexp.MustCompile(`(?s)/\*.*?\*/`).ReplaceAllString(s, "")
	// usuń linie zaczynające się od -- (np. "-- name: ...")
	s = regexp.MustCompile(`(?m)^[ \t]*--.*$`).ReplaceAllString(s, "")
	// usuń SQL_CALC_FOUND_ROWS (stary mysql-izm potrafi psuć parser)
	s = regexp.MustCompile(`(?i)\bsql_calc_found_rows\b`).ReplaceAllString(s, "")
	// uprość LIKE BINARY → LIKE (parserowi to obojętne, a często tu się wywala)
	s = regexp.MustCompile(`(?i)\blike\s+binary\b`).ReplaceAllString(s, "LIKE")
	s = regexp.MustCompile(`(?i)\bnot\s+like\s+binary\b`).ReplaceAllString(s, "NOT LIKE")

	s = strings.TrimSpace(s)
	// przytnij do pierwszego SELECT (gdy plik ma nagłówki)
	if i := strings.Index(strings.ToLower(s), "select"); i >= 0 {
		s = s[i:]
	}
	// zdejmij końcowy średnik
	s = strings.TrimRight(s, " \t\r\n;")
	return s
}

func leftMostAliased(te sqlparser.TableExpr) *sqlparser.AliasedTableExpr {
	for {
		switch t := te.(type) {
		case *sqlparser.JoinTableExpr:
			te = t.LeftExpr
		case *sqlparser.AliasedTableExpr:
			return t
		default:
			return nil
		}
	}
}

var joinCounter int

func collectJoins(te sqlparser.TableExpr, dst map[string]JoinPart) {
	switch t := te.(type) {
	case *sqlparser.JoinTableExpr:
		// rekurencja w lewo
		collectJoins(t.LeftExpr, dst)

		rightAlias := aliasOf(t.RightExpr)
		leftAlias := aliasOf(t.LeftExpr)
		if leftAlias == "" {
			leftAlias = lastAliasFromExpr(t.LeftExpr)
		}

		joinType := strings.ToUpper(strings.TrimSpace(t.Join))
		if joinType == "" {
			joinType = "JOIN"
		}
		right := sqlparser.String(t.RightExpr)

		cond := ""
		if t.Condition.On != nil {
			cond = " ON " + sqlparser.String(t.Condition.On)
		} else if len(t.Condition.Using) > 0 {
			cond = " USING " + sqlparser.String(t.Condition.Using)
		}

		joinStr := joinType + " " + right + cond

		key := strings.ToLower(rightAlias)
		if key == "" {
			if tbl, _ := tableAndAlias(t.RightExpr); tbl != "" {
				key = strings.ToLower(tbl)
			}
		}

		if key != "" {
			joinCounter++
			dst[key] = JoinPart{
				Alias:     key,
				JoinText:  joinStr,
				DependsOn: strings.ToLower(leftAlias),
				Order:     joinCounter,
			}
		}

		// rekurencja w prawo
		collectJoins(t.RightExpr, dst)
	}
}
func lastAliasFromExpr(te sqlparser.TableExpr) string {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		return aliasOf(t)
	case *sqlparser.JoinTableExpr:
		// idź w prawo, bo ostatni alias siedzi w prawym joinie
		return lastAliasFromExpr(t.RightExpr)
	}
	return ""
}

func aliasOf(te sqlparser.TableExpr) string {
	if a, ok := te.(*sqlparser.AliasedTableExpr); ok {
		return a.As.String()
	}
	return ""
}

func tableAndAlias(te sqlparser.TableExpr) (table, alias string) {
	if a, ok := te.(*sqlparser.AliasedTableExpr); ok {
		alias = a.As.String()
		switch e := a.Expr.(type) {
		case sqlparser.TableName:
			table = e.Name.String()
			if q := e.Qualifier.String(); q != "" {
				table = q + "." + table
			}
		case *sqlparser.Subquery:
			table = "(subquery)"
		}
	}
	return
}

func sortedKeys(m map[string]JoinPart) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

var logFile = "/tmp/sqlc-plugin.log"

func mylog(txt string) {
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		// fallback na stderr jeśli nie uda się otworzyć pliku
		fmt.Fprintln(os.Stderr, "log error:", err)
		return
	}
	defer f.Close()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	fmt.Fprintf(f, "[%s] %s\n", timestamp, txt)
}
