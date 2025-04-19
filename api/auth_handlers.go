package api

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/zucced/goquery/config"
	"github.com/zucced/goquery/middleware"
	"github.com/zucced/goquery/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// SignupRequest represents the request body for signup
type SignupRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	Name     string `json:"name"`
}

// LoginRequest represents the request body for login
type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// AuthResponse represents the response for authentication endpoints
type AuthResponse struct {
	Token string       `json:"token"`
	User  *models.User `json:"user"`
}

// SignupHandler handles user registration
func SignupHandler(cfg *config.Config) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Parse request body
		var req SignupRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body",
			})
		}

		// Validate required fields
		if req.Email == "" || req.Password == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Email and password are required",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create user
		user, err := models.CreateUser(ctx, req.Email, req.Password, req.Name)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		// Generate JWT token
		token, err := middleware.GenerateToken(user.ID, cfg)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to generate token",
			})
		}

		// Return response
		return c.Status(fiber.StatusCreated).JSON(AuthResponse{
			Token: token,
			User:  user,
		})
	}
}

// LoginHandler handles user login
func LoginHandler(cfg *config.Config) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Parse request body
		var req LoginRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body",
			})
		}

		// Validate required fields
		if req.Email == "" || req.Password == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Email and password are required",
			})
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get user by email
		user, err := models.GetUserByEmail(ctx, req.Email)
		if err != nil || user == nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "Invalid email or password",
			})
		}

		// Verify password
		if !models.VerifyPassword(user.PasswordHash, req.Password) {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "Invalid email or password",
			})
		}

		// Generate JWT token
		token, err := middleware.GenerateToken(user.ID, cfg)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to generate token",
			})
		}

		// Return response
		return c.JSON(AuthResponse{
			Token: token,
			User:  user,
		})
	}
}

// MeHandler returns the current authenticated user
func MeHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get user ID from context
		userID := c.Locals("user_id").(primitive.ObjectID)

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get user by ID
		user, err := models.GetUserByID(ctx, userID)
		if err != nil || user == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "User not found",
			})
		}

		// Return user
		return c.JSON(user)
	}
}
