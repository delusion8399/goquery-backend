package models

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/zucced/goquery/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// QueryResult represents a row in the query results
type QueryResult map[string]interface{}

// MarshalJSON implements the json.Marshaler interface for QueryResult
// to handle special values like NaN and Infinity
func (qr QueryResult) MarshalJSON() ([]byte, error) {
	// Create a sanitized copy of the map
	sanitized := make(map[string]interface{})
	for k, v := range qr {
		sanitized[k] = sanitizeJSONValue(v)
	}

	// Use the standard JSON marshaler on the sanitized map
	return json.Marshal(sanitized)
}

// sanitizeJSONValue handles special values like NaN and Infinity that can't be serialized to JSON
func sanitizeJSONValue(value interface{}) interface{} {
	// Check for float64 NaN or Infinity
	if f, ok := value.(float64); ok {
		if math.IsNaN(f) {
			return "NaN" // Convert NaN to string
		}
		if math.IsInf(f, 1) {
			return "Infinity" // Convert positive infinity to string
		}
		if math.IsInf(f, -1) {
			return "-Infinity" // Convert negative infinity to string
		}
	}

	// Handle maps recursively
	if m, ok := value.(map[string]interface{}); ok {
		result := make(map[string]interface{})
		for k, v := range m {
			result[k] = sanitizeJSONValue(v)
		}
		return result
	}

	// Handle slices recursively
	if s, ok := value.([]interface{}); ok {
		result := make([]interface{}, len(s))
		for i, v := range s {
			result[i] = sanitizeJSONValue(v)
		}
		return result
	}

	// Return the value as is for other types
	return value
}

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

// MarshalJSON implements the json.Marshaler interface for Query
// to handle special values like NaN and Infinity in the Results field
func (q Query) MarshalJSON() ([]byte, error) {
	type Alias Query // Use a type alias to avoid infinite recursion

	// Convert the Query to the alias type
	aliasValue := Alias(q)

	// Marshal the alias value
	return json.Marshal(aliasValue)
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

// ExecuteQuery executes a query against the specified database
func ExecuteQuery(db *Database, query string) ([]QueryResult, string, error) {
	startTime := time.Now()

	switch db.Type {
	case "postgresql":
		return executePostgresQuery(db, query, startTime)
	case "mongodb":
		return executeMongoDBQuery(db, query, startTime)
	default:
		return nil, "", fmt.Errorf("unsupported database type: %s", db.Type)
	}
}
