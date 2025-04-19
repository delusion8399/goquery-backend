package ai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/zucced/goquery/config"
	"github.com/zucced/goquery/models"
)

const (
	OpenRouterBaseURL = "https://openrouter.ai/api/v1/chat/completions"
)

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

// GenerateSQL generates SQL from a natural language query using OpenRouter's Gemini model
func GenerateSQL(naturalQuery string, db *models.Database, cfg *config.Config) (string, error) {
	// Get API key from config
	apiKey := cfg.OpenRouterAPIKey
	if apiKey == "" {
		return "", fmt.Errorf("OpenRouter API key not configured")
	}

	// Create schema description
	var schemaDesc strings.Builder
	schemaDesc.WriteString("Database Schema:\n")

	if db.Schema != nil {
		for _, table := range db.Schema.Tables {
			schemaDesc.WriteString(fmt.Sprintf("Table: %s\n", table.Name))
			schemaDesc.WriteString("Columns:\n")

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
			}
			schemaDesc.WriteString("\n")
		}
	}

	// Create prompt
	prompt := fmt.Sprintf(`You are an expert SQL query generator for %s databases.
Given the following database schema and natural language query, generate a valid SQL query.
Only return the SQL query without any explanation or markdown formatting.
Only use SQL syntax and functions that are compatible with %s databases.
Do not use any database-specific functions or syntax that is not supported by %s.

%s

Natural Language Query: %s

SQL Query:`, db.Type, db.Type, db.Type, schemaDesc.String(), naturalQuery)

	// Create request
	request := OpenRouterRequest{
		Model: "google/gemini-pro", // Using Gemini Pro model
		Messages: []OpenRouterChatMessage{
			{
				Role:    "user",
				Content: prompt,
			},
		},
	}

	fmt.Println(prompt)

	// Convert request to JSON
	requestBody, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %v", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", OpenRouterBaseURL, bytes.NewBuffer(requestBody))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("HTTP-Referer", "https://goquery.io") // Replace with your actual domain

	// Send request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %v", err)
	}

	// Check status code
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var response OpenRouterResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %v", err)
	}

	// Check if we have any choices
	if len(response.Choices) == 0 {
		return "", fmt.Errorf("no response from the model")
	}

	// Get the generated SQL
	generatedSQL := response.Choices[0].Message.Content

	// Clean up the SQL (remove any markdown formatting if present)
	generatedSQL = strings.TrimSpace(generatedSQL)
	generatedSQL = strings.TrimPrefix(generatedSQL, "```sql")
	generatedSQL = strings.TrimPrefix(generatedSQL, "```")
	generatedSQL = strings.TrimSuffix(generatedSQL, "```")
	generatedSQL = strings.TrimSpace(generatedSQL)

	return generatedSQL, nil
}
