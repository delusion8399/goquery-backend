package models

import (
	"context"
	"errors"
	"time"

	"github.com/zucced/goquery/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/crypto/bcrypt"
)

// User represents a user in the system
type User struct {
	ID           primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Email        string             `json:"email" bson:"email"`
	PasswordHash string             `json:"-" bson:"password_hash"`
	Name         string             `json:"name" bson:"name"`
	CreatedAt    time.Time          `json:"created_at" bson:"created_at"`
	UpdatedAt    time.Time          `json:"updated_at" bson:"updated_at"`
}

// UserCollection returns the users collection
func UserCollection() *mongo.Collection {
	return database.GetCollection("users")
}

// CreateUser creates a new user
func CreateUser(ctx context.Context, email, password, name string) (*User, error) {
	// Check if user already exists
	existingUser, _ := GetUserByEmail(ctx, email)
	if existingUser != nil {
		return nil, errors.New("user with this email already exists")
	}

	// Hash the password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}

	// Create the user
	now := time.Now()
	user := &User{
		Email:        email,
		PasswordHash: string(hashedPassword),
		Name:         name,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	// Insert the user into the database
	result, err := UserCollection().InsertOne(ctx, user)
	if err != nil {
		return nil, err
	}

	// Set the ID
	user.ID = result.InsertedID.(primitive.ObjectID)

	return user, nil
}

// GetUserByEmail retrieves a user by email
func GetUserByEmail(ctx context.Context, email string) (*User, error) {
	var user User
	err := UserCollection().FindOne(ctx, bson.M{"email": email}).Decode(&user)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &user, nil
}

// GetUserByID retrieves a user by ID
func GetUserByID(ctx context.Context, id primitive.ObjectID) (*User, error) {
	var user User
	err := UserCollection().FindOne(ctx, bson.M{"_id": id}).Decode(&user)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &user, nil
}

// UpdateUser updates a user
func UpdateUser(ctx context.Context, user *User) error {
	user.UpdatedAt = time.Now()

	_, err := UserCollection().UpdateOne(
		ctx,
		bson.M{"_id": user.ID},
		bson.M{"$set": bson.M{
			"email":      user.Email,
			"name":       user.Name,
			"updated_at": user.UpdatedAt,
		}},
	)
	return err
}

// VerifyPassword checks if the provided password matches the stored hash
func VerifyPassword(hashedPassword, password string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
	return err == nil
}

// UpdatePassword updates a user's password
func UpdatePassword(ctx context.Context, userID primitive.ObjectID, password string) error {
	// Hash the password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	// Update the password
	_, err = UserCollection().UpdateOne(
		ctx,
		bson.M{"_id": userID},
		bson.M{"$set": bson.M{
			"password_hash": string(hashedPassword),
			"updated_at":    time.Now(),
		}},
	)
	return err
}
