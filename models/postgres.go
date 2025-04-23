package models

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/lib/pq" // PostgreSQL driver
)

// getPostgresConnectionString returns a connection string for PostgreSQL
func getPostgresConnectionString(db *Database) string {
	sslMode := "disable"
	if db.SSL {
		sslMode = "require"
	}

	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		db.Host,
		db.Port,
		db.Username,
		db.Password,
		db.DatabaseName,
		sslMode,
	)
}

// testPostgresConnection tests the connection to a PostgreSQL database
func testPostgresConnection(db *Database) error {
	connStr := getPostgresConnectionString(db)
	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open connection: %v", err)
	}
	defer conn.Close()

	// Test the connection
	err = conn.Ping()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	return nil
}

// fetchPostgresSchema fetches the schema of a PostgreSQL database
func fetchPostgresSchema(db *Database) (*Schema, error) {
	connStr := getPostgresConnectionString(db)

	// Set a connection timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open connection with context
	connector, err := pq.NewConnector(connStr)
	if err != nil {
		return &Schema{Tables: []Table{}}, fmt.Errorf("failed to create connector: %v", err)
	}

	conn := sql.OpenDB(connector)
	defer conn.Close()

	// Test the connection
	if err := conn.PingContext(ctx); err != nil {
		return &Schema{Tables: []Table{}}, fmt.Errorf("failed to ping database: %v", err)
	}

	// Query to get all tables in the public schema
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
		AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return &Schema{Tables: []Table{}}, fmt.Errorf("failed to query tables: %v", err)
	}
	defer rows.Close()

	var tables []Table
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return &Schema{Tables: []Table{}}, fmt.Errorf("failed to scan table name: %v", err)
		}

		// Get columns for this table
		columns, err := fetchPostgresColumns(conn, tableName, ctx)
		if err != nil {
			// Log the error but continue with other tables
			log.Printf("Error fetching columns for table %s: %v", tableName, err)
			continue
		}

		tables = append(tables, Table{
			Name:    tableName,
			Columns: columns,
		})
	}

	// Always return a valid schema with at least an empty tables array
	return &Schema{Tables: tables}, nil
}

// fetchPostgresColumns fetches the columns of a PostgreSQL table
func fetchPostgresColumns(db *sql.DB, tableName string, ctx context.Context) ([]Column, error) {
	// Query to get column information including primary key status
	query := `
		SELECT
			c.column_name,
			c.data_type,
			c.is_nullable = 'YES' as is_nullable,
			pg_constraint.contype = 'p' as is_primary_key
		FROM
			information_schema.columns c
		LEFT JOIN
			information_schema.key_column_usage kcu
			ON c.table_name = kcu.table_name AND c.column_name = kcu.column_name
		LEFT JOIN
			pg_constraint
			ON kcu.constraint_name = pg_constraint.conname
		WHERE
			c.table_name = $1
		ORDER BY
			c.ordinal_position
	`

	rows, err := db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %v", err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var column Column
		var isNullable, isPrimaryKey bool

		if err := rows.Scan(&column.Name, &column.Type, &isNullable, &isPrimaryKey); err != nil {
			return nil, fmt.Errorf("failed to scan column: %v", err)
		}

		column.Nullable = isNullable
		column.PrimaryKey = isPrimaryKey

		columns = append(columns, column)
	}

	return columns, nil
}

// fetchPostgresStats fetches statistics about a PostgreSQL database
func fetchPostgresStats(db *Database) (*DatabaseStats, error) {
	connStr := getPostgresConnectionString(db)

	// Set a connection timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open connection with context
	connector, err := pq.NewConnector(connStr)
	if err != nil {
		return &DatabaseStats{TableCount: 0, Size: "Unknown"}, fmt.Errorf("failed to create connector: %v", err)
	}

	conn := sql.OpenDB(connector)
	defer conn.Close()

	// Test the connection
	if err := conn.PingContext(ctx); err != nil {
		return &DatabaseStats{TableCount: 0, Size: "Unknown"}, fmt.Errorf("failed to ping database: %v", err)
	}

	// Query to get table count
	tableCountQuery := `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = 'public'
		AND table_type = 'BASE TABLE'
	`

	var tableCount int
	err = conn.QueryRowContext(ctx, tableCountQuery).Scan(&tableCount)
	if err != nil {
		return &DatabaseStats{TableCount: 0, Size: "Unknown"}, fmt.Errorf("failed to query table count: %v", err)
	}

	// Query to get database size
	sizeQuery := `
		SELECT pg_database_size(current_database())
	`

	var sizeBytes int64
	err = conn.QueryRowContext(ctx, sizeQuery).Scan(&sizeBytes)
	if err != nil {
		return &DatabaseStats{TableCount: tableCount, Size: "Unknown"}, fmt.Errorf("failed to query database size: %v", err)
	}

	// Format size to human-readable format
	size := formatSize(sizeBytes)

	return &DatabaseStats{
		TableCount: tableCount,
		Size:       size,
	}, nil
}

// executePostgresQuery executes a SQL query against a PostgreSQL database
func executePostgresQuery(db *Database, sqlQuery string, startTime time.Time) ([]QueryResult, string, error) {
	connStr := getPostgresConnectionString(db)

	// Set a connection timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open connection with context
	connector, err := pq.NewConnector(connStr)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create connector: %v", err)
	}

	conn := sql.OpenDB(connector)
	defer conn.Close()

	// Test the connection
	if err := conn.PingContext(ctx); err != nil {
		return nil, "", fmt.Errorf("failed to ping database: %v", err)
	}

	// Execute the query
	rows, err := conn.QueryContext(ctx, sqlQuery)
	if err != nil {
		return nil, "", fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, "", fmt.Errorf("failed to get column names: %v", err)
	}

	// Prepare result slice
	var results []QueryResult

	// Iterate through rows
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		// Initialize the pointers
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the slice of pointers
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, "", fmt.Errorf("failed to scan row: %v", err)
		}

		// Create a map for this row
		row := make(QueryResult)

		// Convert each value to its appropriate type and add to the map
		for i, col := range columns {
			val := values[i]

			// Handle nil values
			if val == nil {
				row[col] = nil
				continue
			}

			// Handle different types
			switch v := val.(type) {
			case []byte:
				// Try to convert []byte to string
				row[col] = string(v)
			default:
				row[col] = v
			}
		}

		// Add the row to the results
		results = append(results, row)
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("error iterating over rows: %v", err)
	}

	// Calculate execution time
	executionTime := time.Since(startTime).String()

	return results, executionTime, nil
}
