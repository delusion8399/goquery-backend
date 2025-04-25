package models

import (
	"context"
	"fmt"
	"time"

	"github.com/zucced/goquery/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// Column represents a database column
type Column struct {
	Name       string   `json:"name" bson:"name"`
	Type       string   `json:"type" bson:"type"`
	Nullable   bool     `json:"nullable" bson:"nullable"`
	PrimaryKey bool     `json:"primary_key" bson:"primary_key"`
	Fields     []Column `json:"fields,omitempty" bson:"fields,omitempty"` // For nested fields in MongoDB
	Path       string   `json:"path,omitempty" bson:"path,omitempty"`     // Full path for nested fields
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
	ConnectionURI string             `json:"connection_uri,omitempty" bson:"connection_uri,omitempty"`
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
			"connection_uri": db.ConnectionURI,
			"schema":         db.Schema,
			"stats":          db.Stats,
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

// TestConnection tests the connection to the database
func TestConnection(db *Database) error {
	switch db.Type {
	case "postgresql":
		return testPostgresConnection(db)
	case "mongodb":
		return testMongoDBConnection(db)
	default:
		return fmt.Errorf("unsupported database type: %s", db.Type)
	}
}

// FetchDatabaseSchema fetches the schema of the database
func FetchDatabaseSchema(db *Database) (*Schema, error) {
	switch db.Type {
	case "postgresql":
		return fetchPostgresSchema(db)
	case "mongodb":
		return fetchMongoDBSchema(db)
	default:
		return &Schema{Tables: []Table{}}, fmt.Errorf("unsupported database type: %s", db.Type)
	}
}

// FetchDatabaseStats fetches statistics about the database
func FetchDatabaseStats(db *Database) (*DatabaseStats, error) {
	switch db.Type {
	case "postgresql":
		return fetchPostgresStats(db)
	case "mongodb":
		return fetchMongoDBStats(db)
	default:
		return &DatabaseStats{TableCount: 0, Size: "Unknown"}, fmt.Errorf("unsupported database type: %s", db.Type)
	}
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
