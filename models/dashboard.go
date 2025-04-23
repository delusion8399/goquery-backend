package models

import (
	"context"
	"time"

	"github.com/zucced/goquery/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CardPosition represents the position of a card in a dashboard
type CardPosition struct {
	X int `json:"x" bson:"x"`
	Y int `json:"y" bson:"y"`
	W int `json:"w" bson:"w"`
	H int `json:"h" bson:"h"`
}

// CardType represents the type of a dashboard card
type CardType string

const (
	CardTypeQuery CardType = "query"
	CardTypeChart CardType = "chart"
)

// ChartType represents the type of chart for a card
type ChartType string

const (
	ChartTypeTable ChartType = "table"
	ChartTypeBar   ChartType = "bar"
	ChartTypeLine  ChartType = "line"
	ChartTypePie   ChartType = "pie"
	ChartTypeArea  ChartType = "area"
)

// DashboardCard represents a card in a dashboard
type DashboardCard struct {
	ID        primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Title     string             `json:"title" bson:"title"`
	Type      CardType           `json:"type" bson:"type"`
	QueryID   primitive.ObjectID `json:"query_id,omitempty" bson:"query_id,omitempty"`
	ChartType ChartType          `json:"chart_type,omitempty" bson:"chart_type,omitempty"`
	Position  CardPosition       `json:"position" bson:"position"`
	CreatedAt time.Time          `json:"created_at" bson:"created_at"`
	UpdatedAt time.Time          `json:"updated_at" bson:"updated_at"`
}

// Dashboard represents a user dashboard
type Dashboard struct {
	ID          primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	UserID      primitive.ObjectID `json:"user_id" bson:"user_id"`
	Name        string             `json:"name" bson:"name"`
	Description string             `json:"description,omitempty" bson:"description,omitempty"`
	Cards       []DashboardCard    `json:"cards" bson:"cards"`
	IsDefault   bool               `json:"is_default" bson:"is_default"`
	CreatedAt   time.Time          `json:"created_at" bson:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at" bson:"updated_at"`
}

// DashboardCollection returns the dashboards collection
func DashboardCollection() *mongo.Collection {
	return database.GetCollection("dashboards")
}

// CreateDashboard creates a new dashboard
func CreateDashboard(ctx context.Context, dashboard *Dashboard) (*Dashboard, error) {
	// Set timestamps
	now := time.Now()
	dashboard.CreatedAt = now
	dashboard.UpdatedAt = now

	// Initialize cards array if nil
	if dashboard.Cards == nil {
		dashboard.Cards = []DashboardCard{}
	}

	// Set card IDs and timestamps
	for i := range dashboard.Cards {
		dashboard.Cards[i].ID = primitive.NewObjectID()
		dashboard.Cards[i].CreatedAt = now
		dashboard.Cards[i].UpdatedAt = now
	}

	// Insert the dashboard into the collection
	result, err := DashboardCollection().InsertOne(ctx, dashboard)
	if err != nil {
		return nil, err
	}

	// Set the ID
	dashboard.ID = result.InsertedID.(primitive.ObjectID)

	return dashboard, nil
}

// GetDashboardByID retrieves a dashboard by ID
func GetDashboardByID(ctx context.Context, id primitive.ObjectID) (*Dashboard, error) {
	var dashboard Dashboard
	err := DashboardCollection().FindOne(ctx, bson.M{"_id": id}).Decode(&dashboard)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &dashboard, nil
}

// GetDashboardsByUserID retrieves all dashboards for a user
func GetDashboardsByUserID(ctx context.Context, userID primitive.ObjectID) ([]*Dashboard, error) {
	// Create options for sorting
	opts := options.Find().SetSort(bson.M{"created_at": -1}) // Sort by created_at descending (newest first)

	// Execute the query
	cursor, err := DashboardCollection().Find(ctx, bson.M{"user_id": userID}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var dashboards []*Dashboard
	if err := cursor.All(ctx, &dashboards); err != nil {
		return nil, err
	}

	return dashboards, nil
}

// UpdateDashboard updates a dashboard
func UpdateDashboard(ctx context.Context, dashboard *Dashboard) error {
	dashboard.UpdatedAt = time.Now()

	_, err := DashboardCollection().UpdateOne(
		ctx,
		bson.M{"_id": dashboard.ID},
		bson.M{"$set": dashboard},
	)
	return err
}

// DeleteDashboard deletes a dashboard
func DeleteDashboard(ctx context.Context, id primitive.ObjectID) error {
	_, err := DashboardCollection().DeleteOne(ctx, bson.M{"_id": id})
	return err
}

// AddCardToDashboard adds a card to a dashboard
func AddCardToDashboard(ctx context.Context, dashboardID primitive.ObjectID, card *DashboardCard) error {
	// Set card ID and timestamps
	now := time.Now()
	card.ID = primitive.NewObjectID()
	card.CreatedAt = now
	card.UpdatedAt = now

	// Add the card to the dashboard
	_, err := DashboardCollection().UpdateOne(
		ctx,
		bson.M{"_id": dashboardID},
		bson.M{
			"$push": bson.M{"cards": card},
			"$set":  bson.M{"updated_at": now},
		},
	)
	return err
}

// UpdateDashboardCard updates a card in a dashboard
func UpdateDashboardCard(ctx context.Context, dashboardID, cardID primitive.ObjectID, updates map[string]interface{}) error {
	now := time.Now()
	updates["updated_at"] = now

	// Create the update fields with proper dot notation for nested documents
	updateFields := bson.M{}
	for key, value := range updates {
		updateFields["cards.$." + key] = value
	}

	// Update the card
	_, err := DashboardCollection().UpdateOne(
		ctx,
		bson.M{
			"_id":      dashboardID,
			"cards._id": cardID,
		},
		bson.M{
			"$set": updateFields,
		},
	)
	return err
}

// DeleteDashboardCard deletes a card from a dashboard
func DeleteDashboardCard(ctx context.Context, dashboardID, cardID primitive.ObjectID) error {
	now := time.Now()

	// Remove the card from the dashboard
	_, err := DashboardCollection().UpdateOne(
		ctx,
		bson.M{"_id": dashboardID},
		bson.M{
			"$pull": bson.M{"cards": bson.M{"_id": cardID}},
			"$set":  bson.M{"updated_at": now},
		},
	)
	return err
}

// UpdateCardPositions updates the positions of multiple cards in a dashboard
func UpdateCardPositions(ctx context.Context, dashboardID primitive.ObjectID, cardPositions map[primitive.ObjectID]CardPosition) error {
	now := time.Now()

	// Get the dashboard
	dashboard, err := GetDashboardByID(ctx, dashboardID)
	if err != nil {
		return err
	}

	// Update card positions
	for i, card := range dashboard.Cards {
		if position, ok := cardPositions[card.ID]; ok {
			dashboard.Cards[i].Position = position
			dashboard.Cards[i].UpdatedAt = now
		}
	}

	dashboard.UpdatedAt = now

	// Update the dashboard
	return UpdateDashboard(ctx, dashboard)
}
