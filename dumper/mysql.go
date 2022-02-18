package dumper

import (
	"database/sql"
	"fmt"
	"io"
	"strings"
)

// ExtendedInsertDefaultRowCount: Default rows that will be dumped by each INSERT statement
const (
	// OperationIgnore is used to skip a table when dumping.
	OperationIgnore = "ignore"
	// OperationNoData is used when you want to dump a table structure without the data.
	OperationNoData = "nodata"

	// DefaultExtendedInsertRows is used when a value is not provided.
	DefaultExtendedInsertRows = 100
)

// Client used for dumping a database and/or table.
type Client struct {
	DB                 *sql.DB
	SelectMap          map[string]map[string]string
	WhereMap           map[string]string
	FilterMap          map[string]string
	UseTableLock       bool
	ExtendedInsertRows int
}

// NewMySQLDumper is the constructor
func NewMySQLDumper(db *sql.DB) *Client {
	return &Client{
		DB:                 db,
		ExtendedInsertRows: DefaultExtendedInsertRows,
	}
}

// LockTableReading explicitly acquires table locks for the current client session.
func (d *Client) LockTableReading(table string) (sql.Result, error) {
	return d.DB.Exec(fmt.Sprintf("LOCK TABLES `%s` READ", table))
}

// FlushTable will force a tables to be closed.
func (d *Client) FlushTable(table string) (sql.Result, error) {
	return d.DB.Exec(fmt.Sprintf("FLUSH TABLES `%s`", table))
}

// UnlockTables explicitly releases any table locks held by the current session.
func (d *Client) UnlockTables() (sql.Result, error) {
	return d.DB.Exec("UNLOCK TABLES")
}

// GetTables will return a list of tables.
func (d *Client) GetTables() ([]string, error) {
	tables := make([]string, 0)

	rows, err := d.DB.Query("SHOW FULL TABLES")
	if err != nil {
		return tables, err
	}

	defer rows.Close()

	for rows.Next() {
		var tableName, tableType string

		err := rows.Scan(&tableName, &tableType)
		if err != nil {
			return tables, err
		}

		if tableType == "BASE TABLE" {
			tables = append(tables, tableName)
		}
	}

	return tables, nil
}

// WriteCreateTable script used when dumping a database.
func (d *Client) WriteCreateTable(w io.Writer, table string) error {
	fmt.Fprintf(w, "\n--\n-- Structure for table `%s`\n--\n\n", table)
	fmt.Fprintf(w, "DROP TABLE IF EXISTS `%s`;\n", table)

	row := d.DB.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table))

	var name, ddl string

	if err := row.Scan(&name, &ddl); err != nil {
		return err
	}

	fmt.Fprintf(w, "%s;\n", ddl)

	return nil
}

// GetColumnsForSelect for applying the select map from config.
func (d *Client) GetColumnsForSelect(table string) ([]string, error) {
	var rows *sql.Rows

	rows, err := d.DB.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table))
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	for k, column := range columns {
		replacement, ok := d.SelectMap[strings.ToLower(table)][strings.ToLower(column)]
		if ok {
			columns[k] = fmt.Sprintf("%s AS `%s`", replacement, column)
		} else {
			columns[k] = fmt.Sprintf("`%s`", column)
		}
	}

	return columns, nil
}

// GetSelectQueryForTable will return a complete SELECT query to fetch data from a table.
func (d *Client) GetSelectQueryForTable(table string) (string, error) {
	cols, err := d.GetColumnsForSelect(table)
	if err != nil {
		return "", err
	}

	query := fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(cols, ", "), table)

	if where, ok := d.WhereMap[strings.ToLower(table)]; ok {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}

	return query, nil
}

// GetRowCountForTable will return the number of rows using a SELECT statement.
func (d *Client) GetRowCountForTable(table string) (count uint64, err error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
	if where, ok := d.WhereMap[strings.ToLower(table)]; ok {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}
	row := d.DB.QueryRow(query)
	if err = row.Scan(&count); err != nil {
		return
	}
	return
}

// WriteTableLockWrite to be used for a dump script.
func (d *Client) WriteTableLockWrite(w io.Writer, table string) {
	fmt.Fprintf(w, "LOCK TABLES `%s` WRITE;\n", table)
}

// WriteUnlockTables to be used for a dump script.
func (d *Client) WriteUnlockTables(w io.Writer) {
	fmt.Fprintln(w, "UNLOCK TABLES;")
}

// Helper function to get all data for a table.
func (d *Client) selectAllDataFor(table string) (*sql.Rows, []string, error) {
	query, err := d.GetSelectQueryForTable(table)
	if err != nil {
		return nil, nil, err
	}

	rows, err := d.DB.Query(query)
	if err != nil {
		return nil, nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	return rows, columns, nil
}

// WriteTableHeader which contains debug information.
func (d *Client) WriteTableHeader(w io.Writer, table string) (uint64, error) {
	fmt.Fprintf(w, "\n--\n-- Data for table `%s`", table)

	count, err := d.GetRowCountForTable(table)
	if err != nil {
		return 0, err
	}

	fmt.Fprintf(w, " -- %d rows\n--\n\n", count)

	return count, nil
}

// WriteTableData for a specific table.
func (d *Client) WriteTableData(w io.Writer, table string) error {
	rows, columns, err := d.selectAllDataFor(table)
	if err != nil {
		return err
	}

	defer rows.Close()

	values := make([]*sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))

	for i := range values {
		scanArgs[i] = &values[i]
	}

	query := fmt.Sprintf("INSERT INTO `%s` VALUES", table)

	var data []string

	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			return err
		}

		var vals []string

		for _, col := range values {
			val := "NULL"

			if col != nil {
				val = fmt.Sprintf("'%s'", escape(string(*col)))
			}

			vals = append(vals, val)
		}

		data = append(data, fmt.Sprintf("( %s )", strings.Join(vals, ", ")))

		if d.ExtendedInsertRows == 0 {
			continue
		}

		if len(data) >= d.ExtendedInsertRows {
			fmt.Fprintf(w, "%s\n%s;\n", query, strings.Join(data, ",\n"))
			data = make([]string, 0)
		}
	}

	if len(data) > 0 {
		fmt.Fprintf(w, "%s\n%s;\n", query, strings.Join(data, ",\n"))
	}

	return nil
}

// WriteTables will create a script for all tables.
func (d *Client) WriteTables(w io.Writer) error {
	tables, err := d.GetTables()
	if err != nil {
		return err
	}

	for _, table := range tables {
		if err := d.WriteTable(w, table); err != nil {
			return err
		}
	}

	return nil
}

// WriteTable allows for a single table dump script.
func (d *Client) WriteTable(w io.Writer, table string) error {
	if d.FilterMap[strings.ToLower(table)] == OperationIgnore {
		return nil
	}

	skipData := d.FilterMap[strings.ToLower(table)] == OperationNoData
	if !skipData && d.UseTableLock {
		d.LockTableReading(table)
		d.FlushTable(table)
	}

	d.WriteCreateTable(w, table)

	if !skipData {
		cnt, err := d.WriteTableHeader(w, table)
		if err != nil {
			return err
		}

		if cnt > 0 {
			d.WriteTableLockWrite(w, table)
			d.WriteTableData(w, table)

			fmt.Fprintln(w)

			d.WriteUnlockTables(w)
			if d.UseTableLock {
				d.UnlockTables()
			}
		}
	}

	return nil
}
