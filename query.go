package query

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/utils"
)

type DB interface {
	GetGorm() *gorm.DB
	WithGorm(db *gorm.DB) DB
}

// ExecFunc exec func
type ExecFunc = func(db DB, rawSQL DB) (interface{}, error)

// WhereFunc where func
type WhereFunc = func(builder *Builder)

// Pagination ...
type Pagination struct {
	HasNext     bool        `json:"has_next"`
	HasPrev     bool        `json:"has_prev"`
	PerPage     int         `json:"per_page"`
	NextPage    int         `json:"next_page"`
	Page        int         `json:"current_page"`
	PrevPage    int         `json:"prev_page"`
	Offset      int         `json:"offset"`
	Records     interface{} `json:"records"`
	TotalRecord int         `json:"total_record"`
	TotalPage   int         `json:"total_page"`
	Metadata    interface{} `json:"metadata"`
}

// Builder query config
type Builder struct {
	db                DB
	rawSQLString      string
	countRawSQLString string
	limit             int
	page              int
	wrapJSON          bool
	namedWhereValues  map[string]interface{}
	tx                *gorm.DB
}

// New init
func New(db DB, rawSQL string, countRawSQL ...string) *Builder {
	var builder = &Builder{
		db:               db,
		rawSQLString:     rawSQL,
		wrapJSON:         false,
		namedWhereValues: map[string]interface{}{},
		tx:               db.GetGorm().Session(&gorm.Session{DryRun: true}),
	}

	if len(countRawSQL) > 0 {
		builder.countRawSQLString = countRawSQL[0]
	}

	return builder
}

// WithWrapJSON wrap json
func (b *Builder) WithWrapJSON(isWrapJSON bool) *Builder {
	b.wrapJSON = isWrapJSON
	return b
}

// PrepareCountSQL prepare statement
func (b *Builder) count(countSQL *gorm.DB, done chan bool, count *int) {
	var err = countSQL.Scan(count).Error
	if err != nil {
		log.Fatalf("Scan row error: %v", err)
	}
	done <- true
}

// Where where
func (b *Builder) Where(query interface{}, args ...interface{}) *Builder {
	switch value := query.(type) {
	case map[string]interface{}:
		b.namedWhereValues = value
	case map[string]string:
		for key, v := range value {
			b.namedWhereValues[key] = v
		}
	case sql.NamedArg:
		b.namedWhereValues[value.Name] = value.Value
	default:
		b.tx = b.tx.Where(query, args...)

	}

	return b
}

// Having where
func (b *Builder) Having(query interface{}, args ...interface{}) *Builder {
	b.tx.Statement.AddClause(clause.GroupBy{
		Having: b.tx.Statement.BuildCondition(query, args...),
	})
	return b
}

// OrderBy specify order when retrieve records from database
func (b *Builder) OrderBy(orderBy interface{}) *Builder {
	b.tx = b.tx.Order(orderBy)
	return b
}

// GroupBy specify the group method on the find
func (b *Builder) GroupBy(name string) *Builder {
	fields := strings.FieldsFunc(name, utils.IsValidDBNameChar)
	b.tx.Statement.AddClause(clause.GroupBy{
		Columns: []clause.Column{{Name: name, Raw: len(fields) != 1}},
	})
	return b
}

// WhereFunc using where func
func (b *Builder) WhereFunc(f WhereFunc) *Builder {
	f(b)
	return b
}

// Limit limit
func (b *Builder) Limit(limit int) *Builder {
	b.limit = limit
	return b
}

// Page offset
func (b *Builder) Page(page int) *Builder {
	b.page = page
	return b
}

// Build build
func (b *Builder) build() (queryString string, countQuery string, vars []interface{}) {
	queryString = b.rawSQLString
	countQuery = b.countRawSQLString
	if countQuery == "" {
		countQuery = b.rawSQLString
	}
	b.tx.Statement.Build("WHERE", "GROUP BY", "HAVING")

	countQuery = fmt.Sprintf("SELECT COUNT(1) FROM (%s %s) t", countQuery, b.tx.Statement.SQL.String())

	// Build limit, offset clause
	var limitClause = clause.Limit{
		Limit: b.limit,
	}

	if b.page > 0 {
		var offset = 0
		if b.page > 1 {
			offset = (b.page - 1) * b.limit
		}
		limitClause.Offset = offset
	}
	b.tx.Statement.AddClause(limitClause)

	b.tx.Statement.WriteString(" ")
	b.tx.Statement.Build("ORDER BY", "LIMIT")
	queryString = fmt.Sprintf("%s %s", queryString, b.tx.Statement.SQL.String())

	vars = b.tx.Statement.Vars

	for _, vv := range vars {
		var bindvar = strings.Builder{}
		b.db.GetGorm().Dialector.BindVarTo(&bindvar, b.tx.Statement, vv)
		queryString = strings.Replace(queryString, bindvar.String(), "?", 1)
		countQuery = strings.Replace(countQuery, bindvar.String(), "?", 1)

	}

	if b.wrapJSON {
		queryString = fmt.Sprintf(`
WITH alias AS (
	%s
)
SELECT to_jsonb(row_to_json(alias)) AS alias 
FROM alias
		`, queryString)
	}

	return
}

// PagingFunc paging
func (b *Builder) PagingFunc(f ExecFunc) *Pagination {
	if b.page < 1 {
		b.page = 1
	}
	var offset = (b.page - 1) * b.limit
	var done = make(chan bool, 1)
	var pagination Pagination
	var count int

	sqlString, countSQLString, vars := b.build()
	for key, value := range b.namedWhereValues {
		vars = append(vars, sql.Named(key, value))
	}

	var countSQL = b.db.GetGorm().Raw(countSQLString, vars...)
	go b.count(countSQL, done, &count)

	result, err := f(b.db, b.db.WithGorm(b.db.GetGorm().Raw(sqlString, vars...)))
	if err != nil {
		log.Fatalf("Scan row error: %v", err)
		return &pagination
	}

	if reflect.ValueOf(result).Kind() != reflect.Ptr {
		panic("Result of PagingFunc must be a pointer")
	}

	<-done

	pagination.TotalRecord = count
	pagination.Records = result
	pagination.Page = b.page
	pagination.Offset = offset

	if b.limit > 0 {
		pagination.PerPage = b.limit
		pagination.TotalPage = int(math.Ceil(float64(count) / float64(b.limit)))
	} else {
		pagination.TotalPage = 1
		pagination.PerPage = count
	}

	if b.page > 1 {
		pagination.PrevPage = b.page - 1
	} else {
		pagination.PrevPage = b.page
	}

	if b.page == pagination.TotalPage {
		pagination.NextPage = b.page
	} else {
		pagination.NextPage = b.page + 1
	}

	pagination.HasNext = pagination.TotalPage > pagination.Page
	pagination.HasPrev = pagination.Page > 1

	if !pagination.HasNext {
		pagination.NextPage = pagination.Page
	}

	return &pagination
}

// ExecFunc exec
func (b *Builder) ExecFunc(f ExecFunc, dest interface{}) error {
	sqlString, _, vars := b.build()

	for key, value := range b.namedWhereValues {
		vars = append(vars, sql.Named(key, value))
	}

	result, err := f(b.db, b.db.WithGorm(b.db.GetGorm().Raw(sqlString, vars...)))
	if err != nil {
		return err
	}

	var rResult = reflect.ValueOf(result)
	var rOut = reflect.ValueOf(dest)

	if rResult.Kind() != reflect.Ptr {
		rResult = toPtr(rResult)

	}
	if rOut.Kind() != reflect.Ptr {
		rOut = toPtr(rOut)
	}

	if rResult.Type() != rOut.Type() {
		switch rResult.Kind() {
		case reflect.Array, reflect.Slice:
			if rResult.Len() > 0 {
				var elem = rResult.Index(0).Elem()
				rOut.Elem().Set(elem)
				return nil
			}
		}

		panic(fmt.Sprintf("%v is not %v", rResult.Type(), rOut.Type()))
	}

	rOut.Elem().Set(rResult.Elem())

	return nil
}

// Scan scan
func (b *Builder) Scan(dest interface{}) error {
	sqlString, _, vars := b.build()

	var err = b.db.GetGorm().Raw(sqlString, vars...).Scan(dest).Error
	if err != nil {
		return err
	}

	return nil
}

// ScanRow scan
func (b *Builder) ScanRow(dest interface{}) error {
	sqlString, _, vars := b.build()

	var err = b.db.GetGorm().Raw(sqlString, vars...).Row().Scan(dest)
	if err != nil {
		return err
	}

	return nil
}

// toPtr wraps the given value with pointer: V => *V, *V => **V, etc.
func toPtr(v reflect.Value) reflect.Value {
	pt := reflect.PtrTo(v.Type()) // create a *T type.
	pv := reflect.New(pt.Elem())  // create a reflect.Value of type *T.
	pv.Elem().Set(v)              // sets pv to point to underlying value of v.
	return pv
}
