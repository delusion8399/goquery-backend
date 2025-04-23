package api

import (
	"context"
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
		models.UpdateQuery(ctx, query)

		// Execute the query
		results, executionTime, err := models.ExecuteQuery(db, query.GeneratedSQL)
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
