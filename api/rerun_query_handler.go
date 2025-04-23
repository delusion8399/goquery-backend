package api

import (
	"context"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/zucced/goquery/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// RerunQueryHandler handles rerunning an existing query
func RerunQueryHandler() fiber.Handler {
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
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// Get the existing query
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

		// Get the database
		db, err := models.GetDatabaseByID(ctx, query.DatabaseID)
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

		// Update query status
		query.Status = models.QueryStatusRunning
		query.UpdatedAt = time.Now()
		query.Error = "" // Clear any previous errors
		err = models.UpdateQuery(ctx, query)
		if err != nil {
			fmt.Printf("Failed to update query status to running: %v\n", err)
			// Continue anyway
		}

		// Log the query execution
		fmt.Printf("[%s] Rerunning query for database type: %s\n", time.Now().Format(time.RFC3339), db.Type)
		fmt.Printf("Query: %s\n", query.GeneratedSQL)

		// Execute the query based on database type
		fmt.Printf("[%s] Starting query execution\n", time.Now().Format(time.RFC3339))
		executionStartTime := time.Now()
		results, executionTime, err := models.ExecuteQuery(db, query.GeneratedSQL)
		fmt.Printf("[%s] Query execution completed in %s\n", time.Now().Format(time.RFC3339), time.Since(executionStartTime))
		if err != nil {
			// Update query with error
			query.Status = models.QueryStatusFailed
			query.Error = "Failed to execute query: " + err.Error()
			models.UpdateQuery(ctx, query)

			fmt.Printf("Query execution failed: %v\n", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": query.Error,
				"query": query,
			})
		}

		// Update query with results
		query.Status = models.QueryStatusCompleted
		query.Results = results
		query.ExecutionTime = executionTime
		query.Error = "" // Clear any previous errors

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
