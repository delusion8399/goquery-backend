package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/zucced/goquery/api"
	"github.com/zucced/goquery/config"
	"github.com/zucced/goquery/database"
	"github.com/zucced/goquery/middleware"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	fmt.Println("Loaded config: ", cfg)

	// Connect to MongoDB
	if err := database.ConnectDB(cfg); err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer database.DisconnectDB()

	// Create Fiber app
	app := fiber.New(fiber.Config{
		AppName:      "GoQuery API",
		ErrorHandler: errorHandler,
	})

	// Middleware
	app.Use(logger.New())
	app.Use(recover.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: cfg.AllowOrigins,
		AllowHeaders: "Origin, Content-Type, Accept, Authorization",
		AllowMethods: "GET, POST, PUT, DELETE",
	}))

	// Routes
	setupRoutes(app, cfg)

	// Start server
	addr := ":" + strconv.Itoa(cfg.AppPort)
	fmt.Printf("Server is running on http://localhost%s\n", addr)
	if err := app.Listen(addr); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func setupRoutes(app *fiber.App, cfg *config.Config) {
	// API group
	apiGroup := app.Group("/api")

	// Auth routes
	auth := apiGroup.Group("/auth")
	auth.Post("/signup", api.SignupHandler(cfg))
	auth.Post("/login", api.LoginHandler(cfg))
	auth.Get("/me", middleware.AuthMiddleware(cfg), api.MeHandler())

	// Database routes (protected)
	databases := apiGroup.Group("/databases", middleware.AuthMiddleware(cfg))
	databases.Post("", api.CreateDatabaseHandler())
	databases.Get("", api.GetDatabasesHandler())
	databases.Get("/:id", api.GetDatabaseHandler())
	databases.Delete("/:id", api.DeleteDatabaseHandler())
	databases.Post("/test-connection", api.TestConnectionHandler())
	databases.Get("/:id/queries", api.GetDatabaseQueriesHandler())

	// Query routes (protected)
	queries := apiGroup.Group("/queries", middleware.AuthMiddleware(cfg))
	queries.Post("", api.CreateQueryHandler(cfg))
	queries.Get("", api.GetQueriesHandler())
	queries.Get("/:id", api.GetQueryHandler())
	queries.Put("/:id", api.UpdateQueryHandler())
	queries.Delete("/:id", api.DeleteQueryHandler())
	queries.Post("/:id/rerun", api.RerunQueryHandler())

	// Dashboard routes (protected)
	dashboards := apiGroup.Group("/dashboards", middleware.AuthMiddleware(cfg))
	dashboards.Post("", api.CreateDashboardHandler())
	dashboards.Get("", api.GetDashboardsHandler())
	dashboards.Get("/:id", api.GetDashboardHandler())
	dashboards.Put("/:id", api.UpdateDashboardHandler())
	dashboards.Delete("/:id", api.DeleteDashboardHandler())
	dashboards.Post("/:id/cards", api.AddCardHandler())
	dashboards.Put("/:id/cards/:cardId", api.UpdateCardHandler())
	dashboards.Delete("/:id/cards/:cardId", api.DeleteCardHandler())
	dashboards.Put("/:id/cards", api.UpdateCardPositionsHandler())

	// Health check
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status": "ok",
		})
	})
}

func errorHandler(c *fiber.Ctx, err error) error {
	// Default error
	code := fiber.StatusInternalServerError
	message := "Internal Server Error"

	// Check if it's a Fiber error
	if e, ok := err.(*fiber.Error); ok {
		code = e.Code
		message = e.Message
	}

	// Return JSON error
	return c.Status(code).JSON(fiber.Map{
		"error": message,
	})
}
