package models

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/zucced/goquery/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// QueryResult represents a row in the query results
type QueryResult map[string]interface{}

// QueryStatus represents the status of a query
type QueryStatus string

const (
	QueryStatusPending   QueryStatus = "pending"
	QueryStatusRunning   QueryStatus = "running"
	QueryStatusCompleted QueryStatus = "completed"
	QueryStatusFailed    QueryStatus = "failed"
)

// Query represents a database query
type Query struct {
	ID            primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	UserID        primitive.ObjectID `json:"user_id" bson:"user_id"`
	DatabaseID    primitive.ObjectID `json:"database_id" bson:"database_id"`
	Name          string             `json:"name,omitempty" bson:"name,omitempty"`
	NaturalQuery  string             `json:"query" bson:"natural_query"`
	GeneratedSQL  string             `json:"sql,omitempty" bson:"generated_sql,omitempty"`
	Status        QueryStatus        `json:"status" bson:"status"`
	Results       []QueryResult      `json:"results,omitempty" bson:"results,omitempty"`
	Error         string             `json:"error,omitempty" bson:"error,omitempty"`
	ExecutionTime string             `json:"execution_time,omitempty" bson:"execution_time,omitempty"`
	CreatedAt     time.Time          `json:"created_at" bson:"created_at"`
	UpdatedAt     time.Time          `json:"updated_at" bson:"updated_at"`
}

// QueryCollection returns the queries collection
func QueryCollection() *mongo.Collection {
	return database.GetCollection("queries")
}

// CreateQuery creates a new query
func CreateQuery(ctx context.Context, query *Query) (*Query, error) {
	// Set timestamps and initial status
	now := time.Now()
	query.CreatedAt = now
	query.UpdatedAt = now
	query.Status = QueryStatusPending

	// Insert the query into the collection
	result, err := QueryCollection().InsertOne(ctx, query)
	if err != nil {
		return nil, err
	}

	// Set the ID
	query.ID = result.InsertedID.(primitive.ObjectID)

	return query, nil
}

// GetQueryByID retrieves a query by ID
func GetQueryByID(ctx context.Context, id primitive.ObjectID) (*Query, error) {
	var query Query
	err := QueryCollection().FindOne(ctx, bson.M{"_id": id}).Decode(&query)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &query, nil
}

// GetQueriesByUserID retrieves all queries for a user with pagination
func GetQueriesByUserID(ctx context.Context, userID primitive.ObjectID, page, limit int64) ([]*Query, int64, error) {
	// Create a filter for the user ID
	filter := bson.M{"user_id": userID}

	// Count total documents for pagination
	totalCount, err := QueryCollection().CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, err
	}

	// Calculate skip value for pagination
	skip := (page - 1) * limit
	if skip < 0 {
		skip = 0
	}

	// Create options for sorting and pagination
	opts := options.Find().
		SetSort(bson.M{"created_at": -1}). // Sort by created_at descending (newest first)
		SetSkip(skip).
		SetLimit(limit)

	// Execute the query
	cursor, err := QueryCollection().Find(ctx, filter, opts)
	if err != nil {
		return nil, 0, err
	}
	defer cursor.Close(ctx)

	var queries []*Query
	if err := cursor.All(ctx, &queries); err != nil {
		return nil, 0, err
	}

	return queries, totalCount, nil
}

// GetQueriesByDatabaseID retrieves all queries for a specific database with pagination
func GetQueriesByDatabaseID(ctx context.Context, databaseID primitive.ObjectID, page, limit int64) ([]*Query, int64, error) {
	// Create a filter for the database ID
	filter := bson.M{"database_id": databaseID}

	// Count total documents for pagination
	totalCount, err := QueryCollection().CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, err
	}

	// Calculate skip value for pagination
	skip := (page - 1) * limit
	if skip < 0 {
		skip = 0
	}

	// Create options for sorting and pagination
	opts := options.Find().
		SetSort(bson.M{"created_at": -1}). // Sort by created_at descending (newest first)
		SetSkip(skip).
		SetLimit(limit)

	// Execute the query
	cursor, err := QueryCollection().Find(ctx, filter, opts)
	if err != nil {
		return nil, 0, err
	}
	defer cursor.Close(ctx)

	var queries []*Query
	if err := cursor.All(ctx, &queries); err != nil {
		return nil, 0, err
	}

	return queries, totalCount, nil
}

// UpdateQuery updates a query
func UpdateQuery(ctx context.Context, query *Query) error {
	query.UpdatedAt = time.Now()

	_, err := QueryCollection().UpdateOne(
		ctx,
		bson.M{"_id": query.ID},
		bson.M{"$set": query},
	)
	return err
}

// DeleteQuery deletes a query
func DeleteQuery(ctx context.Context, id primitive.ObjectID) error {
	_, err := QueryCollection().DeleteOne(ctx, bson.M{"_id": id})
	return err
}

// ExecuteQuery executes a SQL query against the specified database
func ExecuteQuery(db *Database, sqlQuery string) ([]QueryResult, string, error) {
	startTime := time.Now()

	switch db.Type {
	case "postgresql":
		return executePostgresQuery(db, sqlQuery, startTime)
	default:
		return nil, "", fmt.Errorf("unsupported database type: %s", db.Type)
	}
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

	// Scan rows
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
				// Try to unmarshal as JSON first
				var jsonVal interface{}
				if json.Unmarshal(v, &jsonVal) == nil {
					row[col] = jsonVal
				} else {
					// If not JSON, use as string
					row[col] = string(v)
				}
			default:
				row[col] = v
			}
		}

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
