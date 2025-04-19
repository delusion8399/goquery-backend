package database

import (
	"context"
	"fmt"
	"time"

	"github.com/zucced/goquery/config"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// DB is a global MongoDB client
var DB *mongo.Client

// Database is a global MongoDB database
var Database *mongo.Database

// ConnectDB establishes a connection to the MongoDB database
func ConnectDB(cfg *config.Config) error {
	// Set client options
	clientOptions := options.Client().ApplyURI(cfg.MongoURI)

	// Set connection pool configuration
	clientOptions.SetMaxPoolSize(100)
	clientOptions.SetMinPoolSize(5)
	clientOptions.SetMaxConnIdleTime(30 * time.Minute)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping the database to verify connection
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	// Set global variables
	DB = client
	Database = client.Database(cfg.MongoDatabase)

	fmt.Println("Connected to MongoDB!")
	return nil
}

// DisconnectDB closes the connection to the MongoDB database
func DisconnectDB() error {
	if DB == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := DB.Disconnect(ctx); err != nil {
		return fmt.Errorf("failed to disconnect from MongoDB: %w", err)
	}

	fmt.Println("Disconnected from MongoDB")
	return nil
}

// GetCollection returns a MongoDB collection
func GetCollection(collectionName string) *mongo.Collection {
	return Database.Collection(collectionName)
}
