package ai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/zucced/goquery/config"
	"github.com/zucced/goquery/models"
)

const (
	OpenRouterBaseURL = "https://api.deepseek.com/chat/completions"
)

// addNestedFields recursively adds nested fields to the schema description
func addNestedFields(builder *strings.Builder, fields []models.Column, indent int) {
	indentStr := strings.Repeat(" ", indent)

	for _, field := range fields {
		primaryKey := ""
		if field.PrimaryKey {
			primaryKey = " (PRIMARY KEY)"
		}
		nullable := ""
		if !field.Nullable {
			nullable = " NOT NULL"
		}

		// Add the field with proper indentation
		builder.WriteString(fmt.Sprintf("%s- %s: %s%s%s\n",
			indentStr, field.Name, field.Type, primaryKey, nullable))

		// Recursively add nested fields if any
		if len(field.Fields) > 0 {
			addNestedFields(builder, field.Fields, indent+2)
		}
	}
}

// OpenRouterRequest represents a request to the OpenRouter API
type OpenRouterRequest struct {
	Model    string                  `json:"model"`
	Messages []OpenRouterChatMessage `json:"messages"`
}

// OpenRouterChatMessage represents a message in the OpenRouter chat API
type OpenRouterChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// OpenRouterResponse represents a response from the OpenRouter API
type OpenRouterResponse struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Index   int `json:"index"`
		Message struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

// FindMatchingSchemaTable finds the closest matching schema table for a natural language query
func FindMatchingSchemaTable(naturalQuery string, db *models.Database, cfg *config.Config) (string, error) {
	startTime := time.Now()

	apiKey := cfg.OpenRouterAPIKey
	if apiKey == "" {
		return "", fmt.Errorf("OpenRouter API key not configured")
	}

	// Build a list of table names only
	var tableNames strings.Builder
	tableNames.WriteString("Available Collections/Tables:\n")

	if db.Schema != nil {
		for _, table := range db.Schema.Tables {
			tableNames.WriteString(fmt.Sprintf("- %s\n", table.Name))
		}
	}

	// Create prompt to find the matching table
	prompt := fmt.Sprintf(`You are an expert database query analyzer.
Given a natural language query and a list of available database tables/collections, determine which table is most likely needed to answer the query.
Return ONLY the name of the single most relevant table/collection without any explanation, comments, or formatting.
If multiple tables might be needed, return only the primary/main table that would be in the FROM clause or the main collection for MongoDB.
If no table seems relevant, return the most reasonable guess based on the query semantics.

%s

Natural Language Query: %s

Most Relevant Table/Collection:`, tableNames.String(), naturalQuery)

	request := OpenRouterRequest{
		Model: "deepseek-chat",
		Messages: []OpenRouterChatMessage{
			{
				Role:    "user",
				Content: prompt,
			},
		},
	}

	requestBody, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %v", err)
	}

	req, err := http.NewRequest("POST", OpenRouterBaseURL, bytes.NewBuffer(requestBody))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var response OpenRouterResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if len(response.Choices) == 0 {
		return "", fmt.Errorf("no response from the model")
	}

	matchingTable := strings.TrimSpace(response.Choices[0].Message.Content)
	fmt.Printf("Matching table for query: %s\n", matchingTable)

	generationTime := time.Since(startTime)
	fmt.Printf("Table matching completed in %s\n", generationTime)

	return matchingTable, nil
}

// GenerateSQL generates a database query from a natural language query using OpenRouter's DeepSeek model
// If tableName is provided, only that table's schema will be included in the prompt
func GenerateSQL(naturalQuery string, db *models.Database, cfg *config.Config, tableName string) (string, error) {
	startTime := time.Now()

	apiKey := cfg.OpenRouterAPIKey
	if apiKey == "" {
		return "", fmt.Errorf("OpenRouter API key not configured")
	}

	var schemaDesc strings.Builder
	schemaDesc.WriteString("Database Schema:\n")

	if db.Schema != nil {
		for _, table := range db.Schema.Tables {
			// If tableName is provided, only include that table
			if tableName != "" && table.Name != tableName {
				continue
			}

			schemaDesc.WriteString(fmt.Sprintf("Collection: %s\n", table.Name))
			schemaDesc.WriteString("Fields:\n")

			for _, column := range table.Columns {
				primaryKey := ""
				if column.PrimaryKey {
					primaryKey = " (PRIMARY KEY)"
				}
				nullable := ""
				if !column.Nullable {
					nullable = " NOT NULL"
				}

				schemaDesc.WriteString(fmt.Sprintf("  - %s: %s%s%s\n",
					column.Name, column.Type, primaryKey, nullable))

				// Include nested fields for MongoDB documents
				if len(column.Fields) > 0 && db.Type == "mongodb" {
					addNestedFields(&schemaDesc, column.Fields, 4) // 4 spaces indentation for nested fields
				}
			}
			schemaDesc.WriteString("\n")
		}
	}

	var prompt string
	if db.Type == "mongodb" {
		prompt = fmt.Sprintf(`You are an expert MongoDB query generator for Go applications.
Given the following MongoDB database schema and natural language query, generate Go code that uses the MongoDB Go driver (go.mongodb.org/mongo-driver) to define the query.
Return only the Go code without any explanation, comments, markdown formatting, or backticks.
Strictly use only fields that exist in the provided schema. When a query mentions a field, match it to the closest semantically matching field name from the schema (e.g., if user asks for 'tax', use 'taxAmount' or 'vatAmount' if they exist, but never create non-existent fields like 'tax').
The code must be complete, syntactically correct, and strictly use Go syntax (no JSON notation).
Support complex queries including find with sort, limit, projection, and aggregate pipelines with match, lookup, group, unwind, etc.
Use bson.D, bson.M, or mongo.Pipeline as appropriate for the operation.
Wrap each component in specific placeholders to aid parsing, as shown below.
For find operations, include placeholders for filter, sort, limit, and projection separately.
For aggregate operations, include a placeholder for the pipeline.
For find operations, generate code like:

var collection = "users"
var operation = "find"
*FILTER_START
bson.M{
	"status": "active",
	"age": bson.M{"$gt": 18}
}
*FILTER_END
*SORT_START
bson.D{{"createdAt", -1}}
*SORT_END
*LIMIT_START
10
*LIMIT_END
*PROJECTION_START
bson.D{{"name", 1}, {"email", 1}, {"_id", 0}}
*PROJECTION_END

For aggregate operations, generate code like:

var collection = "orders"
var operation = "aggregate"
*PIPELINE_START
mongo.Pipeline{
	bson.D{{"$match", bson.M{"status": "active"}}},
	bson.D{{"$lookup", bson.M{
		"from": "companies",
		"localField": "companyRef",
		"foreignField": "_id",
		"as": "company"
	}}},
	bson.D{{"$unwind", "$company"}},
	bson.D{{"$group", bson.M{
		"_id": nil,
		"totalOrders": bson.M{"$sum": 1}
	}}}
}
*PIPELINE_END

Database Schema:
%s

Natural Language Query: %s`, schemaDesc.String(), naturalQuery)
	} else {
		prompt = fmt.Sprintf(`You are an expert SQL query generator for %s databases.
Given the following database schema and natural language query, generate a valid SQL query.
Only return the SQL query without any explanation or markdown formatting.
Only use SQL syntax and functions that are compatible with %s databases.
Do not use any database-specific functions or syntax that is not supported by %s.
Strictly use only fields that exist in the provided schema. When a query mentions a field, match it to the closest semantically matching field name from the schema (e.g., if user asks for 'tax', use 'taxAmount' or 'vatAmount' if they exist, but never create non-existent fields like 'tax').

%s

Natural Language Query: %s

SQL Query:`, db.Type, db.Type, db.Type, schemaDesc.String(), naturalQuery)
	}

	request := OpenRouterRequest{
		Model: "deepseek-chat",
		Messages: []OpenRouterChatMessage{
			{
				Role:    "user",
				Content: prompt,
			},
		},
	}

	requestBody, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %v", err)
	}

	req, err := http.NewRequest("POST", OpenRouterBaseURL, bytes.NewBuffer(requestBody))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var response OpenRouterResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if len(response.Choices) == 0 {
		return "", fmt.Errorf("no response from the model")
	}

	generatedQuery := strings.TrimSpace(response.Choices[0].Message.Content)
	fmt.Printf("Generated MongoDB query code:\n%s\n", generatedQuery)

	generationTime := time.Since(startTime)
	fmt.Printf("Query generation completed in %s\n", generationTime)

	return generatedQuery, nil
}
