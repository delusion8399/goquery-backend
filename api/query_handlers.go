package api

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/zucced/goquery/ai"
	"github.com/zucced/goquery/config"
	"github.com/zucced/goquery/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// QueryRequest represents the request body for query operations
type QueryRequest struct {
	DatabaseID string `json:"database_id"`
	Query      string `json:"query"`
	Name       string `json:"name,omitempty"`
}

// CreateQueryHandler handles creating and executing a new query
func CreateQueryHandler(cfg *config.Config) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Parse request body
		var req QueryRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body",
			})
		}

		// Validate required fields
		if req.DatabaseID == "" || req.Query == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Database ID and query are required",
			})
		}

		// Parse database ID
		databaseID, err := primitive.ObjectIDFromHex(req.DatabaseID)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid database ID",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// Get database
		db, err := models.GetDatabaseByID(ctx, databaseID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve database: " + err.Error(),
			})
		}

		if db == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Database not found",
			})
		}

		// Create query with initial values
		query := &models.Query{
			UserID:       userID,
			DatabaseID:   databaseID,
			NaturalQuery: req.Query,
			Status:       models.QueryStatusRunning,
		}

		// If name is not provided, generate one using LLM
		if req.Name == "" {
			generatedName, err := ai.GenerateQueryTitle(req.Query, cfg)
			if err != nil {
				// Just log the error but continue with the query
				fmt.Printf("Failed to generate query title: %v\n", err)
				query.Name = "Database Query"
			} else {
				query.Name = generatedName
			}
		} else {
			query.Name = req.Name
		}

		// Save query to database
		query, err = models.CreateQuery(ctx, query)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to create query: " + err.Error(),
			})
		}

		// Generate SQL using OpenRouter Gemini
		generatedSQL, err := ai.GenerateSQL(req.Query, db, cfg)
		if err != nil {
			// Update query with error
			query.Status = models.QueryStatusFailed
			query.Error = "Failed to generate SQL: " + err.Error()
			models.UpdateQuery(ctx, query)

			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": query.Error,
				"query": query,
			})
		}

		// Update query with generated SQL
		query.GeneratedSQL = generatedSQL

		// Execute the query
		results, executionTime, err := models.ExecuteQuery(db, generatedSQL)
		if err != nil {
			// Update query with error
			query.Status = models.QueryStatusFailed
			query.Error = "Failed to execute query: " + err.Error()
			models.UpdateQuery(ctx, query)

			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": query.Error,
				"query": query,
			})
		}

		// Update query with results
		query.Status = models.QueryStatusCompleted
		query.Results = results
		query.ExecutionTime = executionTime

		// Save updated query
		err = models.UpdateQuery(ctx, query)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to update query: " + err.Error(),
			})
		}

		// Return response
		return c.JSON(query)
	}
}

// GetQueriesHandler handles retrieving all queries for a user with pagination
func GetQueriesHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Get pagination parameters from query
		pageStr := c.Query("page", "1")
		limitStr := c.Query("limit", "10")

		// Parse pagination parameters
		page, err := strconv.ParseInt(pageStr, 10, 64)
		if err != nil || page < 1 {
			page = 1
		}

		limit, err := strconv.ParseInt(limitStr, 10, 64)
		if err != nil || limit < 1 || limit > 100 {
			limit = 10
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get queries with pagination
		queries, totalCount, err := models.GetQueriesByUserID(ctx, userID, page, limit)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve queries: " + err.Error(),
			})
		}

		// Calculate pagination metadata
		totalPages := (totalCount + limit - 1) / limit // Ceiling division

		// Return response with pagination metadata
		return c.JSON(fiber.Map{
			"queries": queries,
			"pagination": fiber.Map{
				"total": totalCount,
				"page":  page,
				"limit": limit,
				"pages": totalPages,
			},
		})
	}
}

// GetQueryHandler handles retrieving a single query
func GetQueryHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Get query ID from params
		queryID, err := primitive.ObjectIDFromHex(c.Params("id"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid query ID",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get query
		query, err := models.GetQueryByID(ctx, queryID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve query: " + err.Error(),
			})
		}

		if query == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Query not found",
			})
		}

		// Check if query belongs to user
		if query.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "You don't have permission to access this query",
			})
		}

		// Return response
		return c.JSON(query)
	}
}

// UpdateQueryHandler handles updating a query
func UpdateQueryHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Get query ID from params
		queryID, err := primitive.ObjectIDFromHex(c.Params("id"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid query ID",
			})
		}

		// Parse request body
		var req QueryRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get query to check ownership
		query, err := models.GetQueryByID(ctx, queryID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve query: " + err.Error(),
			})
		}

		if query == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Query not found",
			})
		}

		// Check if query belongs to user
		if query.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "You don't have permission to update this query",
			})
		}

		// Update query fields
		if req.Name != "" {
			query.Name = req.Name
		}

		if req.Query != "" {
			query.NaturalQuery = req.Query
		}

		// Save updated query
		err = models.UpdateQuery(ctx, query)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to update query: " + err.Error(),
			})
		}

		// Return response
		return c.JSON(query)
	}
}

// DeleteQueryHandler handles deleting a query
func DeleteQueryHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Get query ID from params
		queryID, err := primitive.ObjectIDFromHex(c.Params("id"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid query ID",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get query to check ownership
		query, err := models.GetQueryByID(ctx, queryID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve query: " + err.Error(),
			})
		}

		if query == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Query not found",
			})
		}

		// Check if query belongs to user
		if query.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "You don't have permission to delete this query",
			})
		}

		// Delete query
		err = models.DeleteQuery(ctx, queryID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to delete query: " + err.Error(),
			})
		}

		// Return response
		return c.JSON(fiber.Map{
			"message": "Query deleted successfully",
		})
	}
}
