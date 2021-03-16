package query

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type DB interface {
	Debug() *gorm.DB
	Model(value interface{}) *gorm.DB
	Clauses(conds ...clause.Expression) *gorm.DB
	Table(name string, args ...interface{}) *gorm.DB
	Distinct(args ...interface{}) *gorm.DB
	Select(query interface{}, args ...interface{})
	Omit(columns ...string) *gorm.DB
	Where(query interface{}, args ...interface{}) *gorm.DB
	Not(query interface{}, args ...interface{}) *gorm.DB
	Or(query interface{}, args ...interface{}) *gorm.DB
	Joins(query string, args ...interface{}) *gorm.DB
	Group(name string) *gorm.DB
	Having(query interface{}, args ...interface{}) *gorm.DB
	Order(value interface{}) *gorm.DB
	Limit(limit int) *gorm.DB
	Offset(offset int) *gorm.DB
	Raw(sql string, values ...interface{}) *gorm.DB
	Unscoped() *gorm.DB
	Assign(attrs ...interface{}) *gorm.DB
	Attrs(attrs ...interface{}) *gorm.DB
	Preload(query string, args ...interface{}) *gorm.DB
	Row() *sql.Row
	Rows() (*sql.Rows, error)
	Scan(dest interface{}) *gorm.DB
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
	db               DB
	RawSQLString     string
	limit            int
	page             int
	hasWhere         bool
	whereValues      []interface{}
	namedWhereValues map[string]interface{}
	orderBy          string
	groupBy          string
	wrapJSON         bool
}

// New init
func New(db DB, rawSQL string) *Builder {
	var builder = &Builder{
		db:               db,
		RawSQLString:     rawSQL,
		whereValues:      []interface{}{},
		namedWhereValues: map[string]interface{}{},
		hasWhere:         false,
		orderBy:          "",
		groupBy:          "",
		wrapJSON:         false,
	}
	return builder
}

// WithWrapJSON wrap json
func (b *Builder) WithWrapJSON(isWrapJSON bool) *Builder {
	b.wrapJSON = isWrapJSON
	return b
}

// PrepareCountSQL prepare statement
func (b *Builder) count(countSQL DB, done chan bool, count *int) {
	countSQL.Row().Scan(count)
	done <- true
}

// WhereNamed where
func (b *Builder) WhereNamed(key string, value interface{}) *Builder {
	b.namedWhereValues[key] = value
	return b
}

// Where where
func (b *Builder) Where(query interface{}, args ...interface{}) *Builder {
	if len(args) > 0 {
		b.whereValues = append(b.whereValues, args...)
	}

	if b.hasWhere {
		b.RawSQLString = fmt.Sprintf("%s AND %v", b.RawSQLString, query)
	} else {
		b.RawSQLString = fmt.Sprintf("%s WHERE %v", b.RawSQLString, query)
		b.hasWhere = true
	}
	return b
}

// OrderBy specify order when retrieve records from database
func (b *Builder) OrderBy(orderBy ...string) *Builder {
	if len(orderBy) > 0 {
		b.orderBy = strings.Join(orderBy, ",")
	}
	return b
}

// GroupBy specify the group method on the find
func (b *Builder) GroupBy(groupBy string) *Builder {
	b.groupBy = groupBy
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
func (b *Builder) build() (queryString string, countQuery string) {
	var rawSQLString = b.RawSQLString
	for key, value := range b.namedWhereValues {
		switch v := value.(type) {
		case string:
			rawSQLString = strings.ReplaceAll(rawSQLString, fmt.Sprintf("@%s", key), fmt.Sprintf("'%v'", value))
		case []string:
			var cols = []string{}
			for _, str := range v {
				cols = append(cols, fmt.Sprintf("'%s'", str))
			}
			rawSQLString = strings.ReplaceAll(rawSQLString, fmt.Sprintf("@%s", key), fmt.Sprintf("%v", strings.Join(cols, ",")))
		default:
			rawSQLString = strings.ReplaceAll(rawSQLString, fmt.Sprintf("@%s", key), fmt.Sprintf("%v", value))
		}

	}

	queryString = rawSQLString
	countQuery = rawSQLString
	if b.groupBy != "" {
		queryString = fmt.Sprintf("%s GROUP BY %s", queryString, b.groupBy)
		countQuery = queryString
	}

	if b.orderBy != "" {
		queryString = fmt.Sprintf("%s ORDER BY %s", queryString, b.orderBy)
	}

	if b.limit > 0 {
		queryString = fmt.Sprintf("%s LIMIT %d", queryString, b.limit)
	}

	if b.page > 0 {
		var offset = 0
		if b.page > 1 {
			offset = (b.page - 1) * b.limit
		}

		queryString = fmt.Sprintf("%s OFFSET %d", queryString, offset)
	}

	if b.wrapJSON {
		queryString = fmt.Sprintf(`
		WITH alias AS (%s)
		SELECT to_jsonb(row_to_json(alias)) AS alias FROM alias
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

	sqlString, countSQLString := b.build()

	var values = []interface{}{}
	values = append(values, b.whereValues...)

	var countSQL = b.db.Raw(fmt.Sprintf("SELECT COUNT(1) FROM (%s) t", countSQLString), values...)
	go b.count(countSQL, done, &count)

	result, err := f(b.db, b.dbb.db.Raw(sqlString, values...))
	if err != nil {
		b.db.CustomLogger.Error(err)
	}
	<-done
	close(done)

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
	sqlString, _ := b.build()

	var values = []interface{}{}
	values = append(values, b.whereValues...)

	result, err := f(b.db, b.db.WithGorm(b.db.Raw(sqlString, values...)))
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
	sqlString, _ := b.build()

	var err = b.db.Raw(sqlString, b.whereValues...).Scan(dest).Error
	if err != nil {
		b.db.CustomLogger.Error(err)
		return err
	}

	return nil
}

// ScanRow scan
func (b *Builder) ScanRow(dest interface{}) error {
	sqlString, _ := b.build()

	var err = b.db.Raw(sqlString, b.whereValues...).Row().Scan(dest)
	if err != nil {
		b.db.CustomLogger.Error(err)
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
