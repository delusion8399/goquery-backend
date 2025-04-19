package models

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/lib/pq" // PostgreSQL driver
	"github.com/zucced/goquery/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// Column represents a database column
type Column struct {
	Name       string `json:"name" bson:"name"`
	Type       string `json:"type" bson:"type"`
	Nullable   bool   `json:"nullable" bson:"nullable"`
	PrimaryKey bool   `json:"primary_key" bson:"primary_key"`
}

// Table represents a database table
type Table struct {
	Name    string   `json:"name" bson:"name"`
	Columns []Column `json:"columns" bson:"columns"`
}

// Schema represents a database schema
type Schema struct {
	Tables []Table `json:"tables" bson:"tables"`
}

// DatabaseStats represents statistics about the database
type DatabaseStats struct {
	TableCount int    `json:"table_count" bson:"table_count"`
	Size       string `json:"size" bson:"size"`
}

// Database represents a database connection in the system
type Database struct {
	ID            primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	UserID        primitive.ObjectID `json:"user_id" bson:"user_id"`
	Name          string             `json:"name" bson:"name"`
	Type          string             `json:"type" bson:"type"`
	Host          string             `json:"host" bson:"host"`
	Port          string             `json:"port" bson:"port"`
	Username      string             `json:"username" bson:"username"`
	Password      string             `json:"-" bson:"password"`
	DatabaseName  string             `json:"database_name" bson:"database_name"`
	SSL           bool               `json:"ssl" bson:"ssl"`
	Schema        *Schema            `json:"schema,omitempty" bson:"schema,omitempty"`
	Stats         *DatabaseStats     `json:"stats,omitempty" bson:"stats,omitempty"`
	CreatedAt     time.Time          `json:"created_at" bson:"created_at"`
	UpdatedAt     time.Time          `json:"updated_at" bson:"updated_at"`
	LastConnected *time.Time         `json:"last_connected,omitempty" bson:"last_connected,omitempty"`
}

// DatabaseCollection returns the databases collection
func DatabaseCollection() *mongo.Collection {
	return database.GetCollection("databases")
}

// CreateDatabase creates a new database connection
func CreateDatabase(ctx context.Context, db *Database) (*Database, error) {
	// Set timestamps
	now := time.Now()
	db.CreatedAt = now
	db.UpdatedAt = now

	// Insert the database into the collection
	result, err := DatabaseCollection().InsertOne(ctx, db)
	if err != nil {
		return nil, err
	}

	// Set the ID
	db.ID = result.InsertedID.(primitive.ObjectID)

	return db, nil
}

// GetDatabaseByID retrieves a database by ID
func GetDatabaseByID(ctx context.Context, id primitive.ObjectID) (*Database, error) {
	var db Database
	err := DatabaseCollection().FindOne(ctx, bson.M{"_id": id}).Decode(&db)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &db, nil
}

// GetDatabasesByUserID retrieves all databases for a user
func GetDatabasesByUserID(ctx context.Context, userID primitive.ObjectID) ([]*Database, error) {
	cursor, err := DatabaseCollection().Find(ctx, bson.M{"user_id": userID})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var databases []*Database
	if err := cursor.All(ctx, &databases); err != nil {
		return nil, err
	}

	return databases, nil
}

// UpdateDatabase updates a database
func UpdateDatabase(ctx context.Context, db *Database) error {
	db.UpdatedAt = time.Now()

	_, err := DatabaseCollection().UpdateOne(
		ctx,
		bson.M{"_id": db.ID},
		bson.M{"$set": bson.M{
			"name":           db.Name,
			"type":           db.Type,
			"host":           db.Host,
			"port":           db.Port,
			"username":       db.Username,
			"password":       db.Password,
			"database_name":  db.DatabaseName,
			"ssl":            db.SSL,
			"updated_at":     db.UpdatedAt,
			"last_connected": db.LastConnected,
		}},
	)
	return err
}

// DeleteDatabase deletes a database
func DeleteDatabase(ctx context.Context, id primitive.ObjectID) error {
	_, err := DatabaseCollection().DeleteOne(ctx, bson.M{"_id": id})
	return err
}

// UpdateLastConnected updates the last connected timestamp
func UpdateLastConnected(ctx context.Context, id primitive.ObjectID) error {
	now := time.Now()
	_, err := DatabaseCollection().UpdateOne(
		ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{
			"last_connected": now,
			"updated_at":     now,
		}},
	)
	return err
}

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

// TestConnection tests the connection to the database
func TestConnection(db *Database) error {
	switch db.Type {
	case "postgresql":
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
	default:
		return fmt.Errorf("unsupported database type: %s", db.Type)
	}
}

// FetchDatabaseSchema fetches the schema of the database
func FetchDatabaseSchema(db *Database) (*Schema, error) {
	switch db.Type {
	case "postgresql":
		return fetchPostgresSchema(db)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", db.Type)
	}
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

	// Get all tables in the database with timeout
	rows, err := conn.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
		LIMIT 100 -- Limit to prevent timeout on large databases
	`)

	fmt.Println(rows)
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
		var isNullable bool
		var isPrimaryKey sql.NullBool // Use sql.NullBool to handle NULL values

		if err := rows.Scan(&column.Name, &column.Type, &isNullable, &isPrimaryKey); err != nil {
			return nil, fmt.Errorf("failed to scan column: %v", err)
		}

		column.Nullable = isNullable
		// Only set PrimaryKey to true if the value is valid and true
		column.PrimaryKey = isPrimaryKey.Valid && isPrimaryKey.Bool

		columns = append(columns, column)
	}

	return columns, nil
}

// FetchDatabaseStats fetches statistics about the database
func FetchDatabaseStats(db *Database) (*DatabaseStats, error) {
	switch db.Type {
	case "postgresql":
		return fetchPostgresStats(db)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", db.Type)
	}
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

	// Get table count
	var tableCount int
	err = conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = 'public'
	`).Scan(&tableCount)
	if err != nil {
		return &DatabaseStats{TableCount: 0, Size: "Unknown"}, fmt.Errorf("failed to get table count: %v", err)
	}

	// Get database size
	var sizeBytes int64
	err = conn.QueryRowContext(ctx, `
		SELECT pg_database_size($1)
	`, db.DatabaseName).Scan(&sizeBytes)
	if err != nil {
		// If we can't get the size, at least return the table count
		return &DatabaseStats{TableCount: tableCount, Size: "Unknown"}, fmt.Errorf("failed to get database size: %v", err)
	}

	// Convert size to human-readable format
	size := formatSize(sizeBytes)

	return &DatabaseStats{
		TableCount: tableCount,
		Size:       size,
	}, nil
}

// formatSize converts bytes to a human-readable format
func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	var size string
	switch {
	case bytes < KB:
		size = fmt.Sprintf("%d B", bytes)
	case bytes < MB:
		size = fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	case bytes < GB:
		size = fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes < TB:
		size = fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	default:
		size = fmt.Sprintf("%.2f TB", float64(bytes)/TB)
	}

	return size
}
