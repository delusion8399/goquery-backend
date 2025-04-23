package models

import (
	"context"
	"fmt"
	"log"
	"math"
	"regexp"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// getMongoDBConnectionString returns a connection string for MongoDB
func getMongoDBConnectionString(db *Database) string {
	// Build the connection string
	connStr := fmt.Sprintf("mongodb+srv://%s:%s@%s/%s",
		db.Username,
		db.Password,
		db.Host,
		db.DatabaseName,
	)

	// Add SSL if enabled
	if db.SSL {
		connStr += "?ssl=true"
	}

	connStr += "&retryWrites=true&w=majority"

	return connStr
}

// testMongoDBConnection tests the connection to a MongoDB database
func testMongoDBConnection(db *Database) error {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get connection string
	connStr := getMongoDBConnectionString(db)

	fmt.Println(connStr)

	// Create client options
	clientOptions := options.Client().ApplyURI(connStr)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to create MongoDB client: %v", err)
	}
	defer client.Disconnect(ctx)

	// Ping the database
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	return nil
}

// fetchMongoDBSchema fetches the schema of a MongoDB database
func fetchMongoDBSchema(db *Database) (*Schema, error) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get connection string
	connStr := getMongoDBConnectionString(db)

	// Create client options
	clientOptions := options.Client().ApplyURI(connStr)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return &Schema{Tables: []Table{}}, fmt.Errorf("failed to create MongoDB client: %v", err)
	}
	defer client.Disconnect(ctx)

	// Ping the database
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return &Schema{Tables: []Table{}}, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	// Get database
	database := client.Database(db.DatabaseName)

	// Get collections (equivalent to tables in SQL)
	collections, err := database.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return &Schema{Tables: []Table{}}, fmt.Errorf("failed to list collections: %v", err)
	}

	// Create tables array
	var tables []Table
	for _, collName := range collections {
		// Skip system collections
		if strings.HasPrefix(collName, "system.") {
			continue
		}

		// Get sample document to infer schema
		coll := database.Collection(collName)
		var doc bson.M
		err := coll.FindOne(ctx, bson.M{}).Decode(&doc)

		// If collection is empty, continue with empty columns
		columns := []Column{}

		if err == nil {
			// Extract fields from document
			columns = inferMongoDBColumns(doc)
		} else if err != mongo.ErrNoDocuments {
			// Log error but continue with other collections
			log.Printf("Error fetching sample document for collection %s: %v", collName, err)
		}

		tables = append(tables, Table{
			Name:    collName,
			Columns: columns,
		})
	}

	return &Schema{Tables: tables}, nil
}

// inferMongoDBColumns infers columns from a MongoDB document
func inferMongoDBColumns(doc bson.M) []Column {
	var columns []Column

	for key, value := range doc {
		// Skip _id field
		if key == "_id" {
			columns = append(columns, Column{
				Name:       "_id",
				Type:       "ObjectID",
				Nullable:   false,
				PrimaryKey: true,
			})
			continue
		}

		// Determine type
		dataType := "unknown"
		switch value.(type) {
		case string:
			dataType = "string"
		case int, int32, int64:
			dataType = "number"
		case float32, float64:
			dataType = "number"
		case bool:
			dataType = "boolean"
		case time.Time:
			dataType = "date"
		case bson.A:
			dataType = "array"
		case bson.M:
			dataType = "object"
		case bson.D:
			dataType = "object"
		case nil:
			dataType = "null"
		}

		columns = append(columns, Column{
			Name:       key,
			Type:       dataType,
			Nullable:   true,
			PrimaryKey: false,
		})
	}

	return columns
}

// fetchMongoDBStats fetches statistics about a MongoDB database
func fetchMongoDBStats(db *Database) (*DatabaseStats, error) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get connection string
	connStr := getMongoDBConnectionString(db)

	// Create client options
	clientOptions := options.Client().ApplyURI(connStr)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return &DatabaseStats{TableCount: 0, Size: "Unknown"}, fmt.Errorf("failed to create MongoDB client: %v", err)
	}
	defer client.Disconnect(ctx)

	// Ping the database
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return &DatabaseStats{TableCount: 0, Size: "Unknown"}, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	// Get database
	database := client.Database(db.DatabaseName)

	// Get collections (equivalent to tables in SQL)
	collections, err := database.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return &DatabaseStats{TableCount: 0, Size: "Unknown"}, fmt.Errorf("failed to list collections: %v", err)
	}

	// Count non-system collections
	collectionCount := 0
	for _, collName := range collections {
		if !strings.HasPrefix(collName, "system.") {
			collectionCount++
		}
	}

	// Get database stats
	var stats bson.M
	err = database.RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}, {Key: "scale", Value: 1024 * 1024}}).Decode(&stats)
	if err != nil {
		return &DatabaseStats{TableCount: collectionCount, Size: "Unknown"}, fmt.Errorf("failed to get database stats: %v", err)
	}

	// Extract size
	size := "Unknown"
	if dataSize, ok := stats["dataSize"].(float64); ok {
		// Convert to bytes (MongoDB returns size in MB)
		sizeBytes := int64(dataSize * 1024 * 1024)
		size = formatSize(sizeBytes)
	}

	return &DatabaseStats{
		TableCount: collectionCount,
		Size:       size,
	}, nil
}

// executeMongoDBQuery executes a MongoDB query
func executeMongoDBQuery(db *Database, query string, startTime time.Time) ([]QueryResult, string, error) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get connection string
	connStr := getMongoDBConnectionString(db)

	// Create client options
	clientOptions := options.Client().ApplyURI(connStr)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create MongoDB client: %v", err)
	}
	defer client.Disconnect(ctx)

	// Ping the database
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, "", fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	// Get database
	database := client.Database(db.DatabaseName)

	// Check if the query is Go code (from AI) or a MongoDB query string
	if strings.Contains(query, "collection :=") && strings.Contains(query, "operation :=") {
		// This is Go code generated by the AI
		return executeMongoDBGoCode(database, query, ctx, startTime)
	}

	// If not Go code, try to parse as a traditional MongoDB query string
	// The query is expected to be in the format:
	// db.collection.find({...}) or db.collection.aggregate([...])
	parsedQuery, err := parseMongoDBQuery(query)
	if err != nil {
		return nil, "", err
	}

	// Execute the query
	var results []bson.M
	switch parsedQuery.Operation {
	case "find":
		findOptions, ok := parsedQuery.Options.(*options.FindOptions)
		if !ok {
			findOptions = options.Find()
		}

		// Log the find operation
		fmt.Printf("Executing MongoDB find on collection '%s' with filter: %+v\n",
			parsedQuery.Collection, parsedQuery.Filter)

		cursor, err := database.Collection(parsedQuery.Collection).Find(ctx, parsedQuery.Filter, findOptions)
		if err != nil {
			return nil, "", fmt.Errorf("failed to execute find query: %v", err)
		}
		defer cursor.Close(ctx)

		if err := cursor.All(ctx, &results); err != nil {
			return nil, "", fmt.Errorf("failed to decode results: %v", err)
		}
	case "aggregate":
		aggregateOptions, ok := parsedQuery.Options.(*options.AggregateOptions)
		if !ok {
			aggregateOptions = options.Aggregate()
		}

		// Log the aggregate operation
		fmt.Printf("Executing MongoDB aggregate on collection '%s' with pipeline: %+v\n",
			parsedQuery.Collection, parsedQuery.Pipeline)

		// Convert the pipeline to the correct type if needed
		var pipeline interface{}
		switch p := parsedQuery.Pipeline.(type) {
		case bson.A:
			pipeline = p
		case []interface{}:
			pipeline = p
		case []bson.M:
			// Convert []bson.M to []interface{}
			pipelineArr := make([]interface{}, len(p))
			for i, stage := range p {
				pipelineArr[i] = stage
			}
			pipeline = pipelineArr
		default:
			return nil, "", fmt.Errorf("unsupported pipeline type: %T", parsedQuery.Pipeline)
		}

		cursor, err := database.Collection(parsedQuery.Collection).Aggregate(ctx, pipeline, aggregateOptions)
		if err != nil {
			return nil, "", fmt.Errorf("failed to execute aggregate query: %v", err)
		}
		defer cursor.Close(ctx)

		if err := cursor.All(ctx, &results); err != nil {
			return nil, "", fmt.Errorf("failed to decode results: %v", err)
		}
	default:
		return nil, "", fmt.Errorf("unsupported MongoDB operation: %s", parsedQuery.Operation)
	}

	// Convert results to QueryResult format and sanitize values
	queryResults := make([]QueryResult, len(results))
	for i, result := range results {
		queryResult := make(QueryResult)
		for key, value := range result {
			// Sanitize the value to handle NaN, Infinity, etc.
			queryResult[key] = sanitizeValue(value)
		}
		queryResults[i] = queryResult
	}

	// Calculate execution time
	executionTime := time.Since(startTime).String()

	return queryResults, executionTime, nil
}

// MongoDBParsedQuery represents a parsed MongoDB query
type MongoDBParsedQuery struct {
	Collection string
	Operation  string
	Filter     interface{}
	Pipeline   interface{}
	Options    interface{}
}

// parseAggregatePipeline parses a MongoDB aggregation pipeline string manually
func parseAggregatePipeline(pipelineStr string) bson.A {
	// This is a simplified parser for common aggregation operations
	// It won't handle all complex cases but should work for basic pipelines
	fmt.Printf("Manual parsing of aggregate pipeline: %s\n", pipelineStr)

	// Remove outer brackets and whitespace
	pipelineStr = strings.TrimSpace(pipelineStr)
	pipelineStr = strings.TrimPrefix(pipelineStr, "[")
	pipelineStr = strings.TrimSuffix(pipelineStr, "]")
	pipelineStr = strings.TrimSpace(pipelineStr)

	// Split by stages (each stage is a separate object)
	// This is a simplified approach and may not work for all complex pipelines
	var stages []string
	braceCount := 0
	currentStage := ""

	for i := 0; i < len(pipelineStr); i++ {
		char := pipelineStr[i]

		if char == '{' {
			braceCount++
		} else if char == '}' {
			braceCount--
		}

		currentStage += string(char)

		// If we've completed a stage object and are at a comma or the end
		if braceCount == 0 && (i == len(pipelineStr)-1 || (pipelineStr[i] == '}' && i+1 < len(pipelineStr) && pipelineStr[i+1] == ',')) {
			stages = append(stages, strings.TrimSpace(currentStage))
			currentStage = ""
			// Skip the comma
			if i+1 < len(pipelineStr) && pipelineStr[i+1] == ',' {
				i++
			}
		}
	}

	// Now parse each stage into a bson.M
	pipeline := bson.A{}

	for _, stageStr := range stages {
		// Convert MongoDB operators to a format that can be parsed
		// Replace $operator with "$$operator" to make it valid JSON
		processedStage := processMongoOperators(stageStr)

		// Try to parse the stage
		var stage bson.M
		if err := bson.UnmarshalExtJSON([]byte(processedStage), true, &stage); err != nil {
			fmt.Printf("Error parsing stage '%s': %v\n", processedStage, err)
			continue
		}

		// Convert back the operators
		stage = convertBackMongoOperators(stage)

		pipeline = append(pipeline, stage)
	}

	fmt.Printf("Manually parsed pipeline: %+v\n", pipeline)
	return pipeline
}

// processMongoOperators processes MongoDB operators to make them valid JSON
func processMongoOperators(input string) string {
	// This is a simplified approach
	// Replace $operator with "$operator" to make it valid JSON
	result := input

	// Common MongoDB operators
	operators := []string{"$match", "$group", "$sort", "$project", "$limit", "$skip", "$unwind", "$lookup", "$sum", "$avg", "$min", "$max", "$push"}

	for _, op := range operators {
		// Replace the operator at the beginning of a key
		result = strings.ReplaceAll(result, op+":", "\""+op+"\":")
	}

	return result
}

// convertBackMongoOperators converts the operators back to their original form
func convertBackMongoOperators(doc bson.M) bson.M {
	// This function would convert back any special handling we did
	// In this simplified version, we don't need to do anything
	return doc
}

// executeMongoDBGoCode executes MongoDB queries from Go code generated by AI
func executeMongoDBGoCode(database *mongo.Database, code string, ctx context.Context, startTime time.Time) ([]QueryResult, string, error) {
	// Log a shorter version of the code for debugging
	codeSummary := code
	if len(codeSummary) > 100 {
		codeSummary = codeSummary[:100] + "..."
	}
	fmt.Printf("Executing MongoDB Go code: %s\n", codeSummary)

	// Extract collection name
	collectionRegex := regexp.MustCompile(`collection\s*:=\s*"([^"]+)"`)
	collectionMatches := collectionRegex.FindStringSubmatch(code)
	if len(collectionMatches) < 2 {
		return nil, "", fmt.Errorf("could not find collection name in the generated code")
	}
	collectionName := collectionMatches[1]

	// Extract operation type
	operationRegex := regexp.MustCompile(`operation\s*:=\s*"([^"]+)"`)
	operationMatches := operationRegex.FindStringSubmatch(code)
	if len(operationMatches) < 2 {
		return nil, "", fmt.Errorf("could not find operation type in the generated code")
	}
	operationType := operationMatches[1]

	// Execute based on operation type
	var results []bson.M

	if operationType == "find" {
		// Extract query filter
		queryRegex := regexp.MustCompile(`query\s*:=\s*bson\.M\{([^}]+)\}`)
		queryMatches := queryRegex.FindStringSubmatch(code)
		if len(queryMatches) < 2 {
			// Try empty query
			queryMatches = []string{"", ""}
		}

		// Parse the query filter
		filterStr := "{" + queryMatches[1] + "}"
		filterStr = strings.ReplaceAll(filterStr, "'", "\"")

		// Try to parse the filter
		var filter bson.M
		if filterStr == "{}" {
			filter = bson.M{}
		} else {
			if err := bson.UnmarshalExtJSON([]byte(filterStr), true, &filter); err != nil {
				// If parsing fails, use an empty filter
				fmt.Printf("Error parsing filter '%s': %v. Using empty filter.\n", filterStr, err)
				filter = bson.M{}
			}
		}

		// Execute find operation
		fmt.Printf("Executing MongoDB find on collection '%s' with filter: %+v\n", collectionName, filter)
		cursor, err := database.Collection(collectionName).Find(ctx, filter)
		if err != nil {
			return nil, "", fmt.Errorf("failed to execute find query: %v", err)
		}
		defer cursor.Close(ctx)

		if err := cursor.All(ctx, &results); err != nil {
			return nil, "", fmt.Errorf("failed to decode results: %v", err)
		}
	} else if operationType == "aggregate" {
		// For aggregate, we need to extract the pipeline stages
		// This is more complex, so we'll use a simplified approach

		// Create a direct pipeline with the stages we need
		// This is a more direct approach that handles the specific format we're seeing
		pipeline := bson.A{}

		// Check for $sort stage with bson.D syntax
		sortRegex := regexp.MustCompile(`\{\{"\$sort":\s*bson\.D\{\{"([^"]+)",\s*(-?\d+)\}\}\}\}`)
		sortMatches := sortRegex.FindAllStringSubmatch(code, -1)
		for _, match := range sortMatches {
			if len(match) < 3 {
				continue
			}

			fieldName := match[1] // e.g., "createdAt"
			sortOrder := match[2] // e.g., "-1"

			// Convert sort order to int
			var order int
			if sortOrder == "-1" {
				order = -1
			} else {
				order = 1
			}

			// Create sort stage
			sortStage := bson.M{"$sort": bson.M{fieldName: order}}
			pipeline = append(pipeline, sortStage)
			fmt.Printf("Added $sort stage: %+v\n", sortStage)
		}

		// Check for $limit stage
		limitRegex := regexp.MustCompile(`\{\{"\$limit":\s*(\d+)\}\}`)
		limitMatches := limitRegex.FindAllStringSubmatch(code, -1)
		for _, match := range limitMatches {
			if len(match) < 2 {
				continue
			}

			limitStr := match[1] // e.g., "50"

			// Convert limit to int
			limit := 50 // Default
			fmt.Sscanf(limitStr, "%d", &limit)

			// Create limit stage
			limitStage := bson.M{"$limit": limit}
			pipeline = append(pipeline, limitStage)
			fmt.Printf("Added $limit stage: %+v\n", limitStage)
		}

		// Check for $group stage with bson.M syntax
		groupRegex := regexp.MustCompile(`\{\{"\$group":\s*bson\.M\{"_id":\s*([^,]+),\s*"([^"]+)":\s*bson\.M\{"\$sum":\s*"([^"]+)"\}\}\}\}`)
		groupMatches := groupRegex.FindAllStringSubmatch(code, -1)
		for _, match := range groupMatches {
			if len(match) < 4 {
				continue
			}

			idValue := match[1]   // e.g., "nil"
			fieldName := match[2] // e.g., "totalValue"
			sumField := match[3]  // e.g., "$payment.total"

			// Create group stage
			var groupId interface{}
			if idValue == "nil" {
				groupId = nil
			} else {
				groupId = idValue
			}

			groupStage := bson.M{
				"$group": bson.M{
					"_id":     groupId,
					fieldName: bson.M{"$sum": sumField},
				},
			}
			pipeline = append(pipeline, groupStage)
			fmt.Printf("Added $group stage: %+v\n", groupStage)
		}

		// If no stages were parsed, try the original approach
		if len(pipeline) == 0 {
			// Extract pipeline
			pipelineRegex := regexp.MustCompile(`pipeline\s*:=\s*mongo\.Pipeline\{([^}]+)\}`)
			pipelineMatches := pipelineRegex.FindStringSubmatch(code)
			if len(pipelineMatches) < 2 {
				// Try another pattern
				pipelineRegex = regexp.MustCompile(`pipeline\s*:=\s*mongo\.Pipeline\{([\s\S]+?)\}\s*//`)
				pipelineMatches = pipelineRegex.FindStringSubmatch(code)
				if len(pipelineMatches) < 2 {
					// If we still can't find the pipeline, create a simple one
					fmt.Printf("Could not find pipeline, using a simple pipeline\n")
					pipeline = bson.A{
						bson.M{"$match": bson.M{}},
						bson.M{"$limit": 100},
					}
				}
			}

			// Extract stages
			stagesStr := pipelineMatches[1]
			manualStages := parseAggregatePipeline(stagesStr)
			if len(manualStages) > 0 {
				pipeline = manualStages
			}
		}

		// If we still couldn't parse any stages, create a simple pipeline
		if len(pipeline) == 0 {
			// Create a simple pipeline that returns all documents
			fmt.Printf("No stages parsed, using a simple pipeline\n")
			pipeline = bson.A{
				bson.M{"$match": bson.M{}},
				bson.M{"$limit": 100},
			}
		}

		// Execute aggregate operation
		fmt.Printf("Executing MongoDB aggregate on collection '%s' with pipeline: %+v\n", collectionName, pipeline)
		cursor, err := database.Collection(collectionName).Aggregate(ctx, pipeline)
		if err != nil {
			return nil, "", fmt.Errorf("failed to execute aggregate query: %v", err)
		}
		defer cursor.Close(ctx)

		if err := cursor.All(ctx, &results); err != nil {
			return nil, "", fmt.Errorf("failed to decode results: %v", err)
		}
	} else {
		return nil, "", fmt.Errorf("unsupported MongoDB operation: %s", operationType)
	}

	// Convert results to QueryResult format and sanitize values
	queryResults := make([]QueryResult, len(results))
	for i, result := range results {
		queryResult := make(QueryResult)
		for key, value := range result {
			// Sanitize the value to handle NaN, Infinity, etc.
			queryResult[key] = sanitizeValue(value)
		}
		queryResults[i] = queryResult
	}

	// Calculate execution time
	executionTime := time.Since(startTime).String()

	return queryResults, executionTime, nil
}

// sanitizeValue handles special values like NaN and Infinity that can't be serialized to JSON
func sanitizeValue(value interface{}) interface{} {
	// Check for float64 NaN or Infinity
	if f, ok := value.(float64); ok {
		if math.IsNaN(f) {
			return "NaN" // Convert NaN to string
		}
		if math.IsInf(f, 1) {
			return "Infinity" // Convert positive infinity to string
		}
		if math.IsInf(f, -1) {
			return "-Infinity" // Convert negative infinity to string
		}
	}

	// Handle maps recursively
	if m, ok := value.(map[string]interface{}); ok {
		result := make(map[string]interface{})
		for k, v := range m {
			result[k] = sanitizeValue(v)
		}
		return result
	}

	// Handle slices recursively
	if s, ok := value.([]interface{}); ok {
		result := make([]interface{}, len(s))
		for i, v := range s {
			result[i] = sanitizeValue(v)
		}
		return result
	}

	// Return the value as is for other types
	return value
}

// parseMongoDBQuery parses a MongoDB query string
func parseMongoDBQuery(queryStr string) (*MongoDBParsedQuery, error) {
	// Log the incoming query for debugging
	fmt.Printf("Parsing MongoDB query: %s\n", queryStr)

	// Remove whitespace and db. prefix if present
	queryStr = strings.TrimSpace(queryStr)
	queryStr = strings.TrimPrefix(queryStr, "db.")

	// Split by first dot to get collection name
	parts := strings.SplitN(queryStr, ".", 2)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid MongoDB query format: %s", queryStr)
	}

	collection := parts[0]
	operationWithArgs := parts[1]

	// Split operation and arguments
	opParts := strings.SplitN(operationWithArgs, "(", 2)
	if len(opParts) < 2 {
		return nil, fmt.Errorf("invalid MongoDB query format: %s", queryStr)
	}

	operation := opParts[0]
	argsStr := opParts[1]

	// Remove trailing parenthesis
	argsStr = strings.TrimSuffix(argsStr, ")")

	// Log the parsed components
	fmt.Printf("Parsed MongoDB query - Collection: %s, Operation: %s\n", collection, operation)

	// Parse arguments based on operation
	parsedQuery := &MongoDBParsedQuery{
		Collection: collection,
		Operation:  operation,
	}

	switch operation {
	case "find":
		// Handle empty filter case
		if strings.TrimSpace(argsStr) == "" || strings.TrimSpace(argsStr) == "{}" {
			parsedQuery.Filter = bson.M{}
		} else {
			// Parse filter
			var filter bson.M
			if err := bson.UnmarshalExtJSON([]byte(argsStr), true, &filter); err != nil {
				fmt.Printf("Error parsing MongoDB find filter: %v\n", err)
				return nil, fmt.Errorf("failed to parse MongoDB find filter: %v", err)
			}
			parsedQuery.Filter = filter
		}
		parsedQuery.Options = options.Find()

	case "aggregate":
		// Handle empty pipeline case
		if strings.TrimSpace(argsStr) == "" || strings.TrimSpace(argsStr) == "[]" {
			parsedQuery.Pipeline = bson.A{}
		} else {
			// For aggregate, we need to manually parse the pipeline
			// because MongoDB's aggregation pipeline uses $ operators which can cause issues with JSON parsing

			// First, let's try to convert the string to a valid JSON format
			// Replace single quotes with double quotes if present
			argsStr = strings.ReplaceAll(argsStr, "'", "\"")

			// Try to parse as extended JSON
			var pipeline bson.A
			if err := bson.UnmarshalExtJSON([]byte(argsStr), true, &pipeline); err != nil {
				fmt.Printf("Error parsing MongoDB aggregate pipeline with extended JSON: %v\n", err)

				// If that fails, try a more manual approach for simple cases
				// This is a simplified approach and may not work for all complex pipelines
				pipeline = parseAggregatePipeline(argsStr)
				if len(pipeline) == 0 {
					return nil, fmt.Errorf("failed to parse MongoDB aggregate pipeline: %v", err)
				}
			}
			parsedQuery.Pipeline = pipeline
			fmt.Printf("Parsed pipeline: %+v\n", pipeline)
		}
		parsedQuery.Options = options.Aggregate()

	default:
		return nil, fmt.Errorf("unsupported MongoDB operation: %s", operation)
	}

	return parsedQuery, nil
}
