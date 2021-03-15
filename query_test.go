package query

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type CustomString string

var db *DBTest

type DBTest struct {
	*gorm.DB
}

func (db *DBTest) GetGorm() *gorm.DB {
	return db.DB
}

func (db *DBTest) WithGorm(gdb *gorm.DB) DB {
	db.DB = gdb
	return db
}

// Model model
type Model struct {
	ID        uint  `gorm:"primaryKey" json:"id"`
	UpdatedAt int64 `gorm:"autoUpdateTime" json:"updated_at"`
	CreatedAt int64 `gorm:"autoCreateTime" json:"created_at"`
}

type Profile struct {
	Model
	Avatar string `json:"avatar"`
}

type CreditCard struct {
	Model
	UserID uint   `json:"user_id"`
	Last4  string `json:"last_4"`
}

// User user
type User struct {
	Model
	Email       string        `json:"email"`
	Phone       string        `json:"phone"`
	ProfileID   uint          `json:"profile_id"`
	Profile     *Profile      `json:"profile"`
	CreditCards []*CreditCard `json:"credit_cards"`
}

func TestWrapJSONPagingFunc(t *testing.T) {
	initDB()

	var sql = `
	SELECT u.*, row_to_json(p) AS profile, json_agg(DISTINCT(cc)) FILTER (WHERE cc IS NOT NULL) AS credit_cards
 	FROM users u 
	LEFT JOIN profiles p ON p.id = u.profile_id
	LEFT JOIN credit_cards cc ON cc.user_id = u.id
	`

	var result = New(db, sql).
		WithWrapJSON(true).
		GroupBy("u.id, p.id").
		PagingFunc(func(db, rawSQL DB) (interface{}, error) {
			var users []*User

			rows, err := rawSQL.GetGorm().Rows()
			defer rows.Close()
			if err != nil {
				panic(err)
			}

			var rawData JSON
			for rows.Next() {
				err = db.GetGorm().ScanRows(rows, &rawData)
				if err != nil {
					log.Fatalf("Scan row error: %v", err)
					continue
				}

				var user User
				err = rawData.Alias.Unmarshal(&user)
				if err != nil {
					log.Fatalf("Unmarshal error: %v", err)
					continue
				}

				users = append(users, &user)
			}

			return &users, nil

		})

	printJSON(result)
}

func TestPagingFunc(t *testing.T) {
	initDB()

	var sql = `
	SELECT u.*, row_to_json(p) AS profile, json_agg(cc) AS credit_cards
 	FROM users u 
	LEFT JOIN profiles p ON p.id = u.profile_id
	LEFT JOIN credit_cards cc ON cc.user_id = u.id
	`

	var result = New(db, sql).
		GroupBy("u.id, p.id").
		PagingFunc(func(db, rawSQL DB) (interface{}, error) {
			type UserAlias struct {
				*User
				Profile     datatypes.JSON
				CreditCards datatypes.JSON
			}

			var users []*User

			rows, err := rawSQL.GetGorm().Rows()
			defer rows.Close()
			if err != nil {
				panic(err)
			}

			var userAlias UserAlias
			for rows.Next() {
				err = db.GetGorm().ScanRows(rows, &userAlias)
				if err != nil {
					continue
				}
				jsoniter.Unmarshal(userAlias.Profile, &userAlias.User.Profile)
				jsoniter.Unmarshal(userAlias.CreditCards, &userAlias.User.CreditCards)
				users = append(users, userAlias.User)
			}

			return &users, nil

		})

	printJSON(result)
}

func TestExecFunc(t *testing.T) {
	initDB()

	var sql = `
	SELECT u.*, row_to_json(p) AS profile, json_agg(cc) AS credit_cards
 	FROM users u 
	LEFT JOIN profiles p ON p.id = u.profile_id
	LEFT JOIN credit_cards cc ON cc.user_id = u.id
	`

	var users []*User
	var err = New(db, sql).
		GroupBy("u.id, p.id").
		ExecFunc(func(db, rawSQL DB) (interface{}, error) {
			type UserAlias struct {
				*User
				Profile     datatypes.JSON
				CreditCards datatypes.JSON
			}

			var users []*User

			rows, err := rawSQL.GetGorm().Rows()
			defer rows.Close()
			if err != nil {
				panic(err)
			}

			var userAlias UserAlias
			for rows.Next() {
				if err = db.GetGorm().ScanRows(rows, &userAlias); err != nil {
					continue
				}
				jsoniter.Unmarshal(userAlias.Profile, &userAlias.User.Profile)
				jsoniter.Unmarshal(userAlias.CreditCards, &userAlias.User.CreditCards)
				users = append(users, userAlias.User)
			}

			return &users, nil
		}, &users)
	if err != nil {
		panic(err)
	}
	printJSON(users)
}

func TestScan(t *testing.T) {
	initDB()

	var sql = `
	SELECT COUNT(1) FROM users u
	`

	var totalUser = 0
	var err = New(db, sql).
		GroupBy("u.id").
		Scan(&totalUser)
	if err != nil {
		panic(err)
	}
	printJSON(totalUser)
}

func TestScanStruct(t *testing.T) {
	initDB()

	var sql = `
	SELECT COUNT(1) FILTER (WHERE id < 5) AS total_user1,
	COUNT(1) FILTER (WHERE id > 5) AS total_user2
	FROM users u
	`
	type CountResult struct {
		TotalUser1 int `json:"total_user1"`
		TotalUser2 int `json:"total_user2"`
	}
	var result CountResult

	var err = New(db, sql).
		Scan(&result)
	if err != nil {
		panic(err)
	}
	printJSON(result)
}

func TestScanMap(t *testing.T) {
	initDB()

	var sql = `
	SELECT COUNT(1) FILTER (WHERE id < 5) AS total_user1,
	COUNT(1) FILTER (WHERE id > 5) AS total_user2
	FROM users u
	`

	var result = map[string]interface{}{}

	var err = New(db, sql).
		Scan(&result)
	if err != nil {
		panic(err)
	}
	printJSON(result)
}

func TestScanRow(t *testing.T) {
	initDB()

	var sql = `
	SELECT COUNT(1) FROM users u
	`

	var totalUser = 0
	var err = New(db, sql).
		GroupBy("u.id").
		ScanRow(&totalUser)
	if err != nil {
		panic(err)
	}
	printJSON(totalUser)
}

func initDB() {
	var uri = "host=localhost user=postgres password= dbname=gorm_test port=5432 sslmode=disable"
	gdb, err := gorm.Open(postgres.Open(uri), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	db = &DBTest{
		DB: gdb.Debug(),
	}

}

func mockData() {
	if db.Migrator().HasTable(&User{}) {
		db.Migrator().DropTable(&User{})
	}

	if db.Migrator().HasTable(&Profile{}) {
		db.Migrator().DropTable(&Profile{})
	}

	if db.Migrator().HasTable(&CreditCard{}) {
		db.Migrator().DropTable(&CreditCard{})
	}

	db.AutoMigrate(&CreditCard{})
	db.AutoMigrate(&Profile{})
	db.AutoMigrate(&User{})
	var createUsers = []*User{}
	// Seed database
	for i := 1; i <= 20; i++ {
		var user = User{
			Email: fmt.Sprintf("user_%d@test.com", i),
			Phone: "+12345678910",
			Profile: &Profile{
				Avatar: fmt.Sprintf("avatar_%d", i),
			},
			CreditCards: []*CreditCard{
				{
					Last4: "1111",
				},
				{
					Last4: "1112",
				},
				{
					Last4: "1113",
				},
				{
					Last4: "1114",
				},
				{
					Last4: "1115",
				},
			},
		}
		createUsers = append(createUsers, &user)
	}

	var err = db.Create(&createUsers).Error
	if err != nil {
		panic(err)
	}

}
func printJSON(in interface{}) {
	data, _ := json.MarshalIndent(&in, "", "    ")
	fmt.Println(string(data))
}
