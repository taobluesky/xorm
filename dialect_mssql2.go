// Copyright 2015 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-xorm/core"
)

var (
	mssql2ReservedWords = map[string]bool{
		"ADD":                    true,
		"EXTERNAL":               true,
		"PROCEDURE":              true,
		"ALL":                    true,
		"FETCH":                  true,
		"PUBLIC":                 true,
		"ALTER":                  true,
		"FILE":                   true,
		"RAISERROR":              true,
		"AND":                    true,
		"FILLFACTOR":             true,
		"READ":                   true,
		"ANY":                    true,
		"FOR":                    true,
		"READTEXT":               true,
		"AS":                     true,
		"FOREIGN":                true,
		"RECONFIGURE":            true,
		"ASC":                    true,
		"FREETEXT":               true,
		"REFERENCES":             true,
		"AUTHORIZATION":          true,
		"FREETEXTTABLE":          true,
		"REPLICATION":            true,
		"BACKUP":                 true,
		"FROM":                   true,
		"RESTORE":                true,
		"BEGIN":                  true,
		"FULL":                   true,
		"RESTRICT":               true,
		"BETWEEN":                true,
		"FUNCTION":               true,
		"RETURN":                 true,
		"BREAK":                  true,
		"GOTO":                   true,
		"REVERT":                 true,
		"BROWSE":                 true,
		"GRANT":                  true,
		"REVOKE":                 true,
		"BULK":                   true,
		"GROUP":                  true,
		"RIGHT":                  true,
		"BY":                     true,
		"HAVING":                 true,
		"ROLLBACK":               true,
		"CASCADE":                true,
		"HOLDLOCK":               true,
		"ROWCOUNT":               true,
		"CASE":                   true,
		"IDENTITY":               true,
		"ROWGUIDCOL":             true,
		"CHECK":                  true,
		"IDENTITY_INSERT":        true,
		"RULE":                   true,
		"CHECKPOINT":             true,
		"IDENTITYCOL":            true,
		"SAVE":                   true,
		"CLOSE":                  true,
		"IF":                     true,
		"SCHEMA":                 true,
		"CLUSTERED":              true,
		"IN":                     true,
		"SECURITYAUDIT":          true,
		"COALESCE":               true,
		"INDEX":                  true,
		"SELECT":                 true,
		"COLLATE":                true,
		"INNER":                  true,
		"SEMANTICKEYPHRASETABLE": true,
		"COLUMN":                 true,
		"INSERT":                 true,
		"SEMANTICSIMILARITYDETAILSTABLE": true,
		"COMMIT":                  true,
		"INTERSECT":               true,
		"SEMANTICSIMILARITYTABLE": true,
		"COMPUTE":                 true,
		"INTO":                    true,
		"SESSION_USER":            true,
		"CONSTRAINT":              true,
		"IS":                      true,
		"SET":                     true,
		"CONTAINS":                true,
		"JOIN":                    true,
		"SETUSER":                 true,
		"CONTAINSTABLE":           true,
		"KEY":                     true,
		"SHUTDOWN":                true,
		"CONTINUE":                true,
		"KILL":                    true,
		"SOME":                    true,
		"CONVERT":                 true,
		"LEFT":                    true,
		"STATISTICS":              true,
		"CREATE":                  true,
		"LIKE":                    true,
		"SYSTEM_USER":             true,
		"CROSS":                   true,
		"LINENO":                  true,
		"TABLE":                   true,
		"CURRENT":                 true,
		"LOAD":                    true,
		"TABLESAMPLE":             true,
		"CURRENT_DATE":            true,
		"MERGE":                   true,
		"TEXTSIZE":                true,
		"CURRENT_TIME":            true,
		"NATIONAL":                true,
		"THEN":                    true,
		"CURRENT_TIMESTAMP":       true,
		"NOCHECK":                 true,
		"TO":                      true,
		"CURRENT_USER":            true,
		"NONCLUSTERED":            true,
		"TOP":                     true,
		"CURSOR":                  true,
		"NOT":                     true,
		"TRAN":                    true,
		"DATABASE":                true,
		"NULL":                    true,
		"TRANSACTION":             true,
		"DBCC":                    true,
		"NULLIF":                  true,
		"TRIGGER":                 true,
		"DEALLOCATE":              true,
		"OF":                      true,
		"TRUNCATE":                true,
		"DECLARE":                 true,
		"OFF":                     true,
		"TRY_CONVERT":             true,
		"DEFAULT":                 true,
		"OFFSETS":                 true,
		"TSEQUAL":                 true,
		"DELETE":                  true,
		"ON":                      true,
		"UNION":                   true,
		"DENY":                    true,
		"OPEN":                    true,
		"UNIQUE":                  true,
		"DESC":                    true,
		"OPENDATASOURCE":          true,
		"UNPIVOT":                 true,
		"DISK":                    true,
		"OPENQUERY":               true,
		"UPDATE":                  true,
		"DISTINCT":                true,
		"OPENROWSET":              true,
		"UPDATETEXT":              true,
		"DISTRIBUTED":             true,
		"OPENXML":                 true,
		"USE":                     true,
		"DOUBLE":                  true,
		"OPTION":                  true,
		"USER":                    true,
		"DROP":                    true,
		"OR":                      true,
		"VALUES":                  true,
		"DUMP":                    true,
		"ORDER":                   true,
		"VARYING":                 true,
		"ELSE":                    true,
		"OUTER":                   true,
		"VIEW":                    true,
		"END":                     true,
		"OVER":                    true,
		"WAITFOR":                 true,
		"ERRLVL":                  true,
		"PERCENT":                 true,
		"WHEN":                    true,
		"ESCAPE":                  true,
		"PIVOT":                   true,
		"WHERE":                   true,
		"EXCEPT":                  true,
		"PLAN":                    true,
		"WHILE":                   true,
		"EXEC":                    true,
		"PRECISION":               true,
		"WITH":                    true,
		"EXECUTE":                 true,
		"PRIMARY":                 true,
		"WITHIN":                  true,
		"EXISTS":                  true,
		"PRINT":                   true,
		"WRITETEXT":               true,
		"EXIT":                    true,
		"PROC":                    true,
	}
)

const(
	sqlServer2000 = "8"
)

type mssql2 struct {
	core.Base
	majorVersion string
}

func (db *mssql2) Init(d *core.DB, uri *core.Uri, drivername, dataSourceName string) error {
	return db.Base.Init(d, db, uri, drivername, dataSourceName)
	/*
	if err !=nil {
		return err
	}
	rows, err := db.DB().Query(`select cast(SERVERPROPERTY('ProductVersion') as varchar)`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var productVersion string

		err = rows.Scan(&productVersion)
		if err != nil {
			return err
		}
		kv := strings.Split(productVersion, ".")
		if len(kv) > 1 {
			db.majorVersion = kv[1]
			return nil
		}
	}
	return err*/
}

func (db *mssql2) SqlType(c *core.Column) string {
	var res string
	switch t := c.SQLType.Name; t {
	case core.Bool:
		res = core.Bit
		if strings.EqualFold(c.Default, "true") {
			c.Default = "1"
		} else if strings.EqualFold(c.Default, "false") {
			c.Default = "0"
		}
	case core.Serial:
		c.IsAutoIncrement = true
		c.IsPrimaryKey = true
		c.Nullable = false
		res = core.Int
	case core.BigSerial:
		c.IsAutoIncrement = true
		c.IsPrimaryKey = true
		c.Nullable = false
		res = core.BigInt
	case core.Bytea, core.Blob, core.Binary, core.TinyBlob, core.MediumBlob, core.LongBlob:
		res = core.VarBinary
		if c.Length == 0 {
			c.Length = 50
		}
	case core.TimeStamp:
		res = core.DateTime
	case core.TimeStampz:
		res = "DATETIMEOFFSET"
		c.Length = 7
	case core.MediumInt:
		res = core.Int
	case core.Text, core.MediumText, core.TinyText, core.LongText, core.Json:
		res = core.Varchar + "(MAX)"
	case core.Double:
		res = core.Real
	case core.Uuid:
		res = core.Varchar
		c.Length = 40
	case core.TinyInt:
		res = core.TinyInt
		c.Length = 0
	default:
		res = t
	}

	if res == core.Int {
		return core.Int
	}

	hasLen1 := (c.Length > 0)
	hasLen2 := (c.Length2 > 0)

	if hasLen2 {
		res += "(" + strconv.Itoa(c.Length) + "," + strconv.Itoa(c.Length2) + ")"
	} else if hasLen1 {
		res += "(" + strconv.Itoa(c.Length) + ")"
	}
	return res
}

func (db *mssql2) SupportInsertMany() bool {
	return true
}

func (db *mssql2) IsReserved(name string) bool {
	_, ok := mssql2ReservedWords[name]
	return ok
}

func (db *mssql2) Quote(name string) string {
	return "\"" + name + "\""
}

func (db *mssql2) QuoteStr() string {
	return "\""
}

func (db *mssql2) SupportEngine() bool {
	return false
}

func (db *mssql2) AutoIncrStr() string {
	return "IDENTITY"
}

func (db *mssql2) DropTableSql(tableName string) string {
	return fmt.Sprintf("IF EXISTS (SELECT * FROM sysobjects WHERE id = "+
		"object_id(N'%s') and OBJECTPROPERTY(id, N'IsUserTable') = 1) "+
		"DROP TABLE \"%s\"", tableName, tableName)
}

func (db *mssql2) SupportCharset() bool {
	return false
}

func (db *mssql2) IndexOnTable() bool {
	return true
}

func (db *mssql2) IndexCheckSql(tableName, idxName string) (string, []interface{}) {
	args := []interface{}{idxName}
	sql := "select name from sysindexes where id=object_id('" + tableName + "') and name=?"
	return sql, args
}

/*func (db *mssql2) ColumnCheckSql(tableName, colName string) (string, []interface{}) {
	args := []interface{}{tableName, colName}
	sql := `SELECT "COLUMN_NAME" FROM "INFORMATION_SCHEMA"."COLUMNS" WHERE "TABLE_NAME" = ? AND "COLUMN_NAME" = ?`
	return sql, args
}*/

func (db *mssql2) IsColumnExist(tableName, colName string) (bool, error) {
	query := `SELECT "COLUMN_NAME" FROM "INFORMATION_SCHEMA"."COLUMNS" WHERE "TABLE_NAME" = ? AND "COLUMN_NAME" = ?`

	return db.HasRecords(query, tableName, colName)
}

func (db *mssql2) TableCheckSql(tableName string) (string, []interface{}) {
	args := []interface{}{}
	sql := "select * from sysobjects where id = object_id(N'" + tableName + "') and OBJECTPROPERTY(id, N'IsUserTable') = 1"
	return sql, args
}

func (db *mssql2) GetColumns(tableName string) ([]string, map[string]*core.Column, error) {
	args := []interface{}{}
	s := `SELECT
	a.name AS name,
	b.name AS ctype,
	a.length,
	ISNULL(a.prec,0),
	ISNULL(a.scale,0),
	a.isnullable AS nullable,
	replace( replace( isnull( c.text, '' ), '(', '' ), ')', '' ) AS vdefault,
	case when exists(SELECT 1 FROM sysobjects where xtype='PK' and parent_obj=a.id and name in (
SELECT name FROM sysindexes WHERE indid in(
SELECT indid FROM sysindexkeys WHERE id = a.id AND colid=a.colid)
)) then 1 else 0 end
FROM
	syscolumns a
	LEFT JOIN systypes b ON a.xusertype= b.xusertype
	LEFT JOIN syscomments c ON a.cdefault= c.id
WHERE
	a.id = object_id('` + tableName + `')`
	db.LogSQL(s, args)

	rows, err := db.DB().Query(s, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols := make(map[string]*core.Column)
	colSeq := make([]string, 0)
	for rows.Next() {
		var name, ctype, vdefault string
		var maxLen, precision, scale int
		var nullable, isPK bool
		err = rows.Scan(&name, &ctype, &maxLen, &precision, &scale, &nullable, &vdefault, &isPK)
		if err != nil {
			return nil, nil, err
		}

		col := new(core.Column)
		col.Indexes = make(map[string]int)
		col.Name = strings.Trim(name, "` ")
		col.Nullable = nullable
		col.Default = vdefault
		col.IsPrimaryKey = isPK
		ct := strings.ToUpper(ctype)
		if ct == "DECIMAL" {
			col.Length = precision
			col.Length2 = scale
		} else {
			col.Length = maxLen
		}
		switch ct {
		case "DATETIMEOFFSET":
			col.SQLType = core.SQLType{Name: core.TimeStampz, DefaultLength: 0, DefaultLength2: 0}
		case "NVARCHAR":
			col.SQLType = core.SQLType{Name: core.NVarchar, DefaultLength: 0, DefaultLength2: 0}
		case "IMAGE":
			col.SQLType = core.SQLType{Name: core.VarBinary, DefaultLength: 0, DefaultLength2: 0}
		default:
			if _, ok := core.SqlTypes[ct]; ok {
				col.SQLType = core.SQLType{Name: ct, DefaultLength: 0, DefaultLength2: 0}
			} else {
				return nil, nil, fmt.Errorf("Unknown colType %v for %v - %v", ct, tableName, col.Name)
			}
		}

		if col.SQLType.IsText() || col.SQLType.IsTime() {
			if col.Default != "" {
				col.Default = "'" + col.Default + "'"
			} else {
				if col.DefaultIsEmpty {
					col.Default = "''"
				}
			}
		}
		cols[col.Name] = col
		colSeq = append(colSeq, col.Name)
	}
	return colSeq, cols, nil
}

func (db *mssql2) GetTables() ([]*core.Table, error) {
	args := []interface{}{}
	s := `select name from sysobjects where xtype ='U'`
	db.LogSQL(s, args)

	rows, err := db.DB().Query(s, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]*core.Table, 0)
	for rows.Next() {
		table := core.NewEmptyTable()
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		table.Name = strings.Trim(name, "` ")
		tables = append(tables, table)
	}
	return tables, nil
}

func (db *mssql2) GetIndexes(tableName string) (map[string]*core.Index, error) {
	args := []interface{}{}
	s := `
SELECT 
	a.name AS [INDEX_NAME],
	c.name AS [COLUMN_NAME],
	INDEXPROPERTY( a.id, a.name, 'IsUnique' ) AS [IS_UNIQUE]
FROM sysindexes a
	LEFT JOIN sysindexkeys b ON a.id=b.id AND a.indid=b.indid
	LEFT JOIN syscolumns c ON a.id=c.id AND c.colid=b.colid
	LEFT JOIN sysobjects o ON a.id = o.id
	LEFT JOIN (
	SELECT
		*,
		CAST ( CASE WHEN ( cons2.xtype = 'PK' ) AND ( cons2.id IS NOT NULL ) THEN 1 ELSE 0 END AS BIT ) AS is_primary_key,
		CAST ( CASE WHEN ( cons2.xtype = 'UQ' ) AND ( cons2.id IS NOT NULL ) THEN 1 ELSE 0 END AS BIT ) AS is_unique_constraint 
	FROM
		sysobjects cons2 
	) cons ON a.id = cons.parent_obj 
	AND a.name = cons.name
WHERE 
	a.id=object_id('`+ tableName+`')
	AND c.name IS NOT NULL
	AND a.indid <> 0 
	AND a.indid <> 255
	AND o.type IN ( 'U', 'S' )
	AND (( cons.is_primary_key IS NULL OR cons.is_primary_key = 0) 
		AND ( cons.is_unique_constraint IS NULL OR cons.is_unique_constraint = 0 ))
`
	db.LogSQL(s, args)

	rows, err := db.DB().Query(s, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexes := make(map[string]*core.Index, 0)
	for rows.Next() {
		var indexType int
		var indexName, colName, isUnique string

		err = rows.Scan(&indexName, &colName, &isUnique)
		if err != nil {
			return nil, err
		}

		i, err := strconv.ParseBool(isUnique)
		if err != nil {
			return nil, err
		}

		if i {
			indexType = core.UniqueType
		} else {
			indexType = core.IndexType
		}

		colName = strings.Trim(colName, "` ")
		var isRegular bool
		if strings.HasPrefix(indexName, "IDX_"+tableName) || strings.HasPrefix(indexName, "UQE_"+tableName) {
			indexName = indexName[5+len(tableName):]
			isRegular = true
		}

		var index *core.Index
		var ok bool
		if index, ok = indexes[indexName]; !ok {
			index = new(core.Index)
			index.Type = indexType
			index.Name = indexName
			index.IsRegular = isRegular
			indexes[indexName] = index
		}
		index.AddColumn(colName)
	}
	return indexes, nil
}

func (db *mssql2) CreateTableSql(table *core.Table, tableName, storeEngine, charset string) string {
	var sql string
	if tableName == "" {
		tableName = table.Name
	}

	sql = "IF NOT EXISTS (select name from sysobjects where xtype ='U' and name= '" + tableName + "' ) CREATE TABLE "

	sql += db.QuoteStr() + tableName + db.QuoteStr() + " ("

	pkList := table.PrimaryKeys

	for _, colName := range table.ColumnsSeq() {
		col := table.GetColumn(colName)
		if col.IsPrimaryKey && len(pkList) == 1 {
			sql += col.String(db)
		} else {
			sql += col.StringNoPk(db)
		}
		sql = strings.TrimSpace(sql)
		sql += ", "
	}

	if len(pkList) > 1 {
		sql += "PRIMARY KEY ( "
		sql += strings.Join(pkList, ",")
		sql += " ), "
	}

	sql = sql[:len(sql)-2] + ")"
	sql += ";"
	return sql
}

func (db *mssql2) ForUpdateSql(query string) string {
	return query
}

func (db *mssql2) Filters() []core.Filter {
	return []core.Filter{&core.IdFilter{}, &core.QuoteFilter{}}
}

type adoDriver struct {
}

func (p *adoDriver) Parse(driverName, dataSourceName string) (*core.Uri, error) {
	kv := strings.Split(dataSourceName, ";")
	var dbName string
	for _, c := range kv {
		vv := strings.Split(strings.TrimSpace(c), "=")
		if len(vv) == 2 {
			switch strings.ToLower(vv[0]) {
			case "initial catalog":
				dbName = vv[1]
			}
		}
	}
	if dbName == "" {
		return nil, errors.New("no db name provided")
	}
	return &core.Uri{DbName: dbName, DbType: core.MSSQL}, nil
}
