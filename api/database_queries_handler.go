package api

import (
	"context"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/zucced/goquery/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// GetDatabaseQueriesHandler handles retrieving queries for a specific database with pagination
func GetDatabaseQueriesHandler() fiber.Handler {
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

		// Get database to check ownership
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

		// Check if database belongs to user
		if db.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "You don't have permission to access this database",
			})
		}

		// Get queries for the database with pagination
		queries, totalCount, err := models.GetQueriesByDatabaseID(ctx, databaseID, page, limit)
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
