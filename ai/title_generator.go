package ai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/zucced/goquery/config"
)

// GenerateQueryTitle generates a concise title for a natural language query using OpenRouter's Gemini model
func GenerateQueryTitle(naturalQuery string, cfg *config.Config) (string, error) {
	// Get API key from config
	apiKey := cfg.OpenRouterAPIKey
	if apiKey == "" {
		return "", fmt.Errorf("OpenRouter API key not configured")
	}

	// Create prompt
	prompt := fmt.Sprintf(`Generate a concise, descriptive title (maximum 5 words) for the following database query.
The title should clearly summarize what the query is looking for.
Only return the title without any explanation, quotes, or additional text.

Query: %s

Title:`, naturalQuery)

	// Create request
	request := OpenRouterRequest{
		Model: "google/gemini-2.0-flash-exp:free",
		Messages: []OpenRouterChatMessage{
			{
				Role:    "user",
				Content: prompt,
			},
		},
	}

	// Convert request to JSON
	requestBody, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %v", err)
	}

	// Use base URL from config or fallback to default
	baseURL := cfg.OpenRouterBaseURL
	if baseURL == "" {
		baseURL = "https://api.deepseek.com/chat/completions"
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", baseURL, bytes.NewBuffer(requestBody))
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

	// Get the generated title
	generatedTitle := response.Choices[0].Message.Content

	// Clean up the title
	generatedTitle = strings.TrimSpace(generatedTitle)
	generatedTitle = strings.Trim(generatedTitle, "\"'")

	// If title is empty, provide a default
	if generatedTitle == "" {
		generatedTitle = "Database Query"
	}

	return generatedTitle, nil
}
