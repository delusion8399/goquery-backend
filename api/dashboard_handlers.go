package api

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/zucced/goquery/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// DashboardRequest represents the request body for dashboard operations
type DashboardRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	IsDefault   bool   `json:"is_default"`
}

// DashboardCardRequest represents the request body for dashboard card operations
type DashboardCardRequest struct {
	Title     string             `json:"title"`
	Type      models.CardType    `json:"type"`
	QueryID   string             `json:"query_id,omitempty"`
	ChartType models.ChartType   `json:"chart_type,omitempty"`
	Position  models.CardPosition `json:"position"`
}

// CardPositionRequest represents the request body for updating card positions
type CardPositionRequest struct {
	CardID   string             `json:"id"`
	Position models.CardPosition `json:"position"`
}

// CreateDashboardHandler handles creating a new dashboard
func CreateDashboardHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Parse request body
		var req DashboardRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body",
			})
		}

		// Validate required fields
		if req.Name == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Name is required",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create dashboard
		dashboard := &models.Dashboard{
			UserID:      userID,
			Name:        req.Name,
			Description: req.Description,
			IsDefault:   req.IsDefault,
			Cards:       []models.DashboardCard{},
		}

		// Save dashboard
		dashboard, err := models.CreateDashboard(ctx, dashboard)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to create dashboard: " + err.Error(),
			})
		}

		// Return response
		return c.JSON(dashboard)
	}
}

// GetDashboardsHandler handles retrieving all dashboards for a user
func GetDashboardsHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get dashboards
		dashboards, err := models.GetDashboardsByUserID(ctx, userID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve dashboards: " + err.Error(),
			})
		}

		// Return response
		return c.JSON(fiber.Map{
			"dashboards": dashboards,
		})
	}
}

// GetDashboardHandler handles retrieving a single dashboard
func GetDashboardHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Get dashboard ID from params
		dashboardID, err := primitive.ObjectIDFromHex(c.Params("id"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid dashboard ID",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get dashboard
		dashboard, err := models.GetDashboardByID(ctx, dashboardID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve dashboard: " + err.Error(),
			})
		}

		// Check if dashboard exists
		if dashboard == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Dashboard not found",
			})
		}

		// Check if dashboard belongs to user
		if dashboard.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "You don't have permission to access this dashboard",
			})
		}

		// Return response
		return c.JSON(dashboard)
	}
}

// UpdateDashboardHandler handles updating a dashboard
func UpdateDashboardHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Get dashboard ID from params
		dashboardID, err := primitive.ObjectIDFromHex(c.Params("id"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid dashboard ID",
			})
		}

		// Parse request body
		var req DashboardRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get dashboard
		dashboard, err := models.GetDashboardByID(ctx, dashboardID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve dashboard: " + err.Error(),
			})
		}

		// Check if dashboard exists
		if dashboard == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Dashboard not found",
			})
		}

		// Check if dashboard belongs to user
		if dashboard.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "You don't have permission to update this dashboard",
			})
		}

		// Update dashboard
		dashboard.Name = req.Name
		dashboard.Description = req.Description
		dashboard.IsDefault = req.IsDefault

		// Save dashboard
		if err := models.UpdateDashboard(ctx, dashboard); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to update dashboard: " + err.Error(),
			})
		}

		// Return response
		return c.JSON(dashboard)
	}
}

// DeleteDashboardHandler handles deleting a dashboard
func DeleteDashboardHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Get dashboard ID from params
		dashboardID, err := primitive.ObjectIDFromHex(c.Params("id"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid dashboard ID",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get dashboard
		dashboard, err := models.GetDashboardByID(ctx, dashboardID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve dashboard: " + err.Error(),
			})
		}

		// Check if dashboard exists
		if dashboard == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Dashboard not found",
			})
		}

		// Check if dashboard belongs to user
		if dashboard.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "You don't have permission to delete this dashboard",
			})
		}

		// Delete dashboard
		if err := models.DeleteDashboard(ctx, dashboardID); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to delete dashboard: " + err.Error(),
			})
		}

		// Return response
		return c.JSON(fiber.Map{
			"message": "Dashboard deleted successfully",
		})
	}
}

// AddCardHandler handles adding a card to a dashboard
func AddCardHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Get dashboard ID from params
		dashboardID, err := primitive.ObjectIDFromHex(c.Params("id"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid dashboard ID",
			})
		}

		// Parse request body
		var req DashboardCardRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body",
			})
		}

		// Validate required fields
		if req.Title == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Title is required",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get dashboard
		dashboard, err := models.GetDashboardByID(ctx, dashboardID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve dashboard: " + err.Error(),
			})
		}

		// Check if dashboard exists
		if dashboard == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Dashboard not found",
			})
		}

		// Check if dashboard belongs to user
		if dashboard.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "You don't have permission to modify this dashboard",
			})
		}

		// Create card
		card := &models.DashboardCard{
			Title:     req.Title,
			Type:      req.Type,
			Position:  req.Position,
			ChartType: req.ChartType,
		}

		// Set query ID if provided
		if req.QueryID != "" {
			queryID, err := primitive.ObjectIDFromHex(req.QueryID)
			if err != nil {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": "Invalid query ID",
				})
			}
			card.QueryID = queryID
		}

		// Add card to dashboard
		if err := models.AddCardToDashboard(ctx, dashboardID, card); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to add card to dashboard: " + err.Error(),
			})
		}

		// Return response
		return c.JSON(card)
	}
}

// UpdateCardHandler handles updating a card in a dashboard
func UpdateCardHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Get dashboard ID and card ID from params
		dashboardID, err := primitive.ObjectIDFromHex(c.Params("id"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid dashboard ID",
			})
		}

		cardID, err := primitive.ObjectIDFromHex(c.Params("cardId"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid card ID",
			})
		}

		// Parse request body
		var req DashboardCardRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get dashboard
		dashboard, err := models.GetDashboardByID(ctx, dashboardID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve dashboard: " + err.Error(),
			})
		}

		// Check if dashboard exists
		if dashboard == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Dashboard not found",
			})
		}

		// Check if dashboard belongs to user
		if dashboard.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "You don't have permission to modify this dashboard",
			})
		}

		// Check if card exists in dashboard
		cardExists := false
		for _, card := range dashboard.Cards {
			if card.ID == cardID {
				cardExists = true
				break
			}
		}

		if !cardExists {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Card not found in dashboard",
			})
		}

		// Prepare updates
		updates := map[string]interface{}{
			"title":      req.Title,
			"type":       req.Type,
			"position":   req.Position,
			"chart_type": req.ChartType,
		}

		// Set query ID if provided
		if req.QueryID != "" {
			queryID, err := primitive.ObjectIDFromHex(req.QueryID)
			if err != nil {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": "Invalid query ID",
				})
			}
			updates["query_id"] = queryID
		}

		// Update card
		if err := models.UpdateDashboardCard(ctx, dashboardID, cardID, updates); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to update card: " + err.Error(),
			})
		}

		// Get updated dashboard
		updatedDashboard, err := models.GetDashboardByID(ctx, dashboardID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve updated dashboard: " + err.Error(),
			})
		}

		// Find the updated card
		var updatedCard *models.DashboardCard
		for _, card := range updatedDashboard.Cards {
			if card.ID == cardID {
				updatedCard = &card
				break
			}
		}

		// Return response
		return c.JSON(updatedCard)
	}
}

// DeleteCardHandler handles deleting a card from a dashboard
func DeleteCardHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Get dashboard ID and card ID from params
		dashboardID, err := primitive.ObjectIDFromHex(c.Params("id"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid dashboard ID",
			})
		}

		cardID, err := primitive.ObjectIDFromHex(c.Params("cardId"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid card ID",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get dashboard
		dashboard, err := models.GetDashboardByID(ctx, dashboardID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve dashboard: " + err.Error(),
			})
		}

		// Check if dashboard exists
		if dashboard == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Dashboard not found",
			})
		}

		// Check if dashboard belongs to user
		if dashboard.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "You don't have permission to modify this dashboard",
			})
		}

		// Check if card exists in dashboard
		cardExists := false
		for _, card := range dashboard.Cards {
			if card.ID == cardID {
				cardExists = true
				break
			}
		}

		if !cardExists {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Card not found in dashboard",
			})
		}

		// Delete card
		if err := models.DeleteDashboardCard(ctx, dashboardID, cardID); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to delete card: " + err.Error(),
			})
		}

		// Return response
		return c.JSON(fiber.Map{
			"message": "Card deleted successfully",
		})
	}
}

// UpdateCardPositionsHandler handles updating the positions of multiple cards in a dashboard
func UpdateCardPositionsHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Get dashboard ID from params
		dashboardID, err := primitive.ObjectIDFromHex(c.Params("id"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid dashboard ID",
			})
		}

		// Parse request body
		var req []CardPositionRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get dashboard
		dashboard, err := models.GetDashboardByID(ctx, dashboardID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve dashboard: " + err.Error(),
			})
		}

		// Check if dashboard exists
		if dashboard == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Dashboard not found",
			})
		}

		// Check if dashboard belongs to user
		if dashboard.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "You don't have permission to modify this dashboard",
			})
		}

		// Prepare card positions
		cardPositions := make(map[primitive.ObjectID]models.CardPosition)
		for _, posReq := range req {
			cardID, err := primitive.ObjectIDFromHex(posReq.CardID)
			if err != nil {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": "Invalid card ID: " + posReq.CardID,
				})
			}
			cardPositions[cardID] = posReq.Position
		}

		// Update card positions
		if err := models.UpdateCardPositions(ctx, dashboardID, cardPositions); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to update card positions: " + err.Error(),
			})
		}

		// Get updated dashboard
		updatedDashboard, err := models.GetDashboardByID(ctx, dashboardID)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve updated dashboard: " + err.Error(),
			})
		}

		// Return response
		return c.JSON(updatedDashboard)
	}
}
