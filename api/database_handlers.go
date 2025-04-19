package api

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/zucced/goquery/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// DatabaseRequest represents the request body for database operations
type DatabaseRequest struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	Host         string `json:"host"`
	Port         string `json:"port"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	DatabaseName string `json:"database"`
	SSL          bool   `json:"ssl"`
}

// CreateDatabaseHandler handles creating a new database connection
func CreateDatabaseHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Parse request body
		var req DatabaseRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body",
			})
		}

		// Validate required fields
		if req.Name == "" || req.Type == "" || req.Host == "" || req.DatabaseName == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Name, type, host, and database name are required",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // Increased timeout for schema fetching
		defer cancel()

		// Create database
		db := &models.Database{
			UserID:       userID,
			Name:         req.Name,
			Type:         req.Type,
			Host:         req.Host,
			Port:         req.Port,
			Username:     req.Username,
			Password:     req.Password,
			DatabaseName: req.DatabaseName,
			SSL:          req.SSL,
		}

		// Test connection
		if err := models.TestConnection(db); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Failed to connect to database: " + err.Error(),
			})
		}

		// Fetch schema
		schema, err := models.FetchDatabaseSchema(db)

		fmt.Println(schema)
		if err != nil {
			// Log the error but don't fail the request
			log.Printf("Failed to fetch schema: %v", err)
			// Initialize with empty schema
			db.Schema = &models.Schema{Tables: []models.Table{}}
		} else {
			db.Schema = schema
		}

		// Fetch stats
		stats, err := models.FetchDatabaseStats(db)
		if err != nil {
			// Log the error but don't fail the request
			log.Printf("Failed to fetch stats: %v", err)
		} else {
			db.Stats = stats
		}

		// Update last connected time
		now := time.Now()
		db.LastConnected = &now

		// Save database
		createdDB, err := models.CreateDatabase(ctx, db)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to save database: " + err.Error(),
			})
		}

		// Return response
		return c.Status(fiber.StatusCreated).JSON(createdDB)
	}
}

// GetDatabasesHandler handles retrieving all databases for a user
func GetDatabasesHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get databases
		databases, err := models.GetDatabasesByUserID(ctx, userID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve databases: " + err.Error(),
			})
		}

		// Return response
		return c.JSON(fiber.Map{
			"databases": databases,
		})
	}
}

// GetDatabaseHandler handles retrieving a single database
func GetDatabaseHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Get database ID from params
		databaseID, err := primitive.ObjectIDFromHex(c.Params("id"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid database ID",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get database
		db, err := models.GetDatabaseByID(ctx, databaseID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve database: " + err.Error(),
			})
		}

		// Check if database exists
		if db == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Database not found",
			})
		}

		// Check if database belongs to user
		if db.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "You do not have permission to access this database",
			})
		}

		// Check if refresh parameter is set
		refresh := c.Query("refresh") == "true"
		if refresh {
			// Test connection
			if err := models.TestConnection(db); err != nil {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": "Failed to connect to database: " + err.Error(),
				})
			}

			// Fetch schema
			schema, err := models.FetchDatabaseSchema(db)

			fmt.Println(schema)
			if err != nil {
				// Log the error but don't fail the request
				log.Printf("Failed to fetch schema: %v", err)
				// Initialize with empty schema
				db.Schema = &models.Schema{Tables: []models.Table{}}
			} else {
				db.Schema = schema
			}

			// Fetch stats
			stats, err := models.FetchDatabaseStats(db)
			if err != nil {
				// Log the error but don't fail the request
				log.Printf("Failed to fetch stats: %v", err)
			} else {
				db.Stats = stats
			}

			// Update last connected time
			now := time.Now()
			db.LastConnected = &now

			// Save updated database
			if err := models.UpdateDatabase(ctx, db); err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "Failed to update database: " + err.Error(),
				})
			}
		}

		// Return response
		return c.JSON(db)
	}
}

// UpdateDatabaseHandler handles updating a database
func UpdateDatabaseHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Get database ID from params
		databaseID, err := primitive.ObjectIDFromHex(c.Params("id"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid database ID",
			})
		}

		// Parse request body
		var req DatabaseRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // Increased timeout for schema fetching
		defer cancel()

		// Get database
		db, err := models.GetDatabaseByID(ctx, databaseID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve database: " + err.Error(),
			})
		}

		// Check if database exists
		if db == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Database not found",
			})
		}

		// Check if database belongs to user
		if db.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "You do not have permission to update this database",
			})
		}

		// Update database
		db.Name = req.Name
		db.Type = req.Type
		db.Host = req.Host
		db.Port = req.Port
		db.Username = req.Username
		if req.Password != "" {
			db.Password = req.Password
		}
		db.DatabaseName = req.DatabaseName
		db.SSL = req.SSL

		// Test connection
		if err := models.TestConnection(db); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Failed to connect to database: " + err.Error(),
			})
		}

		// Fetch schema
		schema, err := models.FetchDatabaseSchema(db)
		if err != nil {
			// Log the error but don't fail the request
			log.Printf("Failed to fetch schema: %v", err)
			// Initialize with empty schema
			db.Schema = &models.Schema{Tables: []models.Table{}}
		} else {
			db.Schema = schema
		}

		// Fetch stats
		stats, err := models.FetchDatabaseStats(db)
		if err != nil {
			// Log the error but don't fail the request
			log.Printf("Failed to fetch stats: %v", err)
		} else {
			db.Stats = stats
		}

		// Update last connected time
		now := time.Now()
		db.LastConnected = &now

		// Save database
		if err := models.UpdateDatabase(ctx, db); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to update database: " + err.Error(),
			})
		}

		// Return response
		return c.JSON(db)
	}
}

// DeleteDatabaseHandler handles deleting a database
func DeleteDatabaseHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Get database ID from params
		databaseID, err := primitive.ObjectIDFromHex(c.Params("id"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid database ID",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get database
		db, err := models.GetDatabaseByID(ctx, databaseID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve database: " + err.Error(),
			})
		}

		// Check if database exists
		if db == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Database not found",
			})
		}

		// Check if database belongs to user
		if db.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "You do not have permission to delete this database",
			})
		}

		// Delete database
		if err := models.DeleteDatabase(ctx, databaseID); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to delete database: " + err.Error(),
			})
		}

		// Return response
		return c.JSON(fiber.Map{
			"message": "Database deleted successfully",
		})
	}
}

// TestConnectionHandler handles testing a database connection
func TestConnectionHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Parse request body
		var req DatabaseRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body",
			})
		}

		// Create database object
		db := &models.Database{
			Name:         req.Name,
			Type:         req.Type,
			Host:         req.Host,
			Port:         req.Port,
			Username:     req.Username,
			Password:     req.Password,
			DatabaseName: req.DatabaseName,
			SSL:          req.SSL,
		}

		// Test connection
		if err := models.TestConnection(db); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Failed to connect to database: " + err.Error(),
			})
		}

		// Try to fetch schema and stats for more comprehensive testing
		response := fiber.Map{
			"message": "Connection successful",
		}

		// Fetch schema (but don't fail if it doesn't work)
		schema, err := models.FetchDatabaseSchema(db)
		if err == nil && schema != nil {
			response["table_count"] = len(schema.Tables)
		}

		// Fetch stats (but don't fail if it doesn't work)
		stats, err := models.FetchDatabaseStats(db)
		if err == nil && stats != nil {
			response["database_size"] = stats.Size
		}

		// Return response
		return c.JSON(response)
	}
}
