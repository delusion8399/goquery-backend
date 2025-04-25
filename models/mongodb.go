package models

import (
	"context"
	"fmt"
	"log"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// getMongoDBConnectionString returns a connection string for MongoDB
func getMongoDBConnectionString(db *Database) string {
	if db.Type == "mongodb" && db.ConnectionURI != "" {
		return db.ConnectionURI
	}

	connStr := fmt.Sprintf("mongodb+srv://%s:%s@%s/%s",
		db.Username,
		db.Password,
		db.Host,
		db.DatabaseName,
	)

	if db.SSL {
		connStr += "?ssl=true"
	}

	connStr += "&retryWrites=true&w=majority"
	return connStr
}

// testMongoDBConnection tests the connection to a MongoDB database
func testMongoDBConnection(db *Database) error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	connStr := getMongoDBConnectionString(db)
	clientOptions := options.Client().ApplyURI(connStr)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to create MongoDB client: %v", err)
	}
	defer client.Disconnect(ctx)

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	return nil
}

// fetchMongoDBSchema fetches the schema of a MongoDB database
func fetchMongoDBSchema(db *Database) (*Schema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	connStr := getMongoDBConnectionString(db)
	clientOptions := options.Client().ApplyURI(connStr)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return &Schema{Tables: []Table{}}, fmt.Errorf("failed to create MongoDB client: %v", err)
	}
	defer client.Disconnect(ctx)

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return &Schema{Tables: []Table{}}, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	var dbName string
	if db.ConnectionURI != "" {
		parts := strings.Split(db.ConnectionURI, "/")
		if len(parts) > 3 {
			dbNameParts := strings.Split(parts[len(parts)-1], "?")
			dbName = dbNameParts[0]
		}
	}

	if dbName == "" {
		dbName = db.DatabaseName
	}

	database := client.Database(dbName)
	collections, err := database.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return &Schema{Tables: []Table{}}, fmt.Errorf("failed to list collections: %v", err)
	}

	var tables []Table
	for _, collName := range collections {
		if strings.HasPrefix(collName, "system.") {
			continue
		}

		coll := database.Collection(collName)
		var doc bson.M
		err := coll.FindOne(ctx, bson.M{}).Decode(&doc)

		columns := []Column{}
		if err == nil {
			columns = inferMongoDBColumns(doc)
		} else if err != mongo.ErrNoDocuments {
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
	return inferMongoDBColumnsWithPath(doc, "")
}

// inferMongoDBColumnsWithPath infers columns from a MongoDB document with path tracking
func inferMongoDBColumnsWithPath(doc bson.M, parentPath string) []Column {
	var columns []Column

	for key, value := range doc {
		// Build the full path for this field
		path := key
		if parentPath != "" {
			path = parentPath + "." + key
		}

		if key == "_id" {
			columns = append(columns, Column{
				Name:       "_id",
				Type:       "ObjectID",
				Nullable:   false,
				PrimaryKey: true,
				Path:       path,
			})
			continue
		}

		dataType := "unknown"
		var fields []Column

		switch v := value.(type) {
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
		case primitive.DateTime:
			dataType = "date"
		case primitive.ObjectID:
			dataType = "ObjectID"
		case bson.A:
			dataType = "array"
			// Process array elements if not empty
			if len(v) > 0 {
				// For arrays, we'll try to infer the schema from the first element
				if firstElem, ok := v[0].(bson.M); ok {
					fields = inferMongoDBColumnsWithPath(firstElem, path)
				} else if firstElem, ok := v[0].(bson.D); ok {
					// Convert bson.D to bson.M
					m := bson.M{}
					for _, e := range firstElem {
						m[e.Key] = e.Value
					}
					fields = inferMongoDBColumnsWithPath(m, path)
				}
			}
		case bson.M:
			dataType = "object"
			fields = inferMongoDBColumnsWithPath(v, path)
		case bson.D:
			dataType = "object"
			// Convert bson.D to bson.M
			m := bson.M{}
			for _, e := range v {
				m[e.Key] = e.Value
			}
			fields = inferMongoDBColumnsWithPath(m, path)
		case map[string]interface{}:
			dataType = "object"
			// Convert map to bson.M
			m := bson.M{}
			for k, val := range v {
				m[k] = val
			}
			fields = inferMongoDBColumnsWithPath(m, path)
		case nil:
			dataType = "null"
		}

		columns = append(columns, Column{
			Name:       key,
			Type:       dataType,
			Nullable:   true,
			PrimaryKey: false,
			Fields:     fields,
			Path:       path,
		})
	}

	return columns
}

// fetchMongoDBStats fetches statistics about a MongoDB database
func fetchMongoDBStats(db *Database) (*DatabaseStats, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	connStr := getMongoDBConnectionString(db)
	clientOptions := options.Client().ApplyURI(connStr)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return &DatabaseStats{TableCount: 0, Size: "Unknown"}, fmt.Errorf("failed to create MongoDB client: %v", err)
	}
	defer client.Disconnect(ctx)

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return &DatabaseStats{TableCount: 0, Size: "Unknown"}, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	var dbName string
	if db.ConnectionURI != "" {
		parts := strings.Split(db.ConnectionURI, "/")
		if len(parts) > 3 {
			dbNameParts := strings.Split(parts[len(parts)-1], "?")
			dbName = dbNameParts[0]
		}
	}

	if dbName == "" {
		dbName = db.DatabaseName
	}

	database := client.Database(dbName)
	collections, err := database.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return &DatabaseStats{TableCount: 0, Size: "Unknown"}, fmt.Errorf("failed to list collections: %v", err)
	}

	collectionCount := 0
	for _, collName := range collections {
		if !strings.HasPrefix(collName, "system.") {
			collectionCount++
		}
	}

	var stats bson.M
	err = database.RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}, {Key: "scale", Value: 1024 * 1024}}).Decode(&stats)
	if err != nil {
		return &DatabaseStats{TableCount: collectionCount, Size: "Unknown"}, fmt.Errorf("failed to get database stats: %v", err)
	}

	size := "Unknown"
	if dataSize, ok := stats["dataSize"].(float64); ok {
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
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	connStr := getMongoDBConnectionString(db)
	clientOptions := options.Client().ApplyURI(connStr)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create MongoDB client: %v", err)
	}
	defer client.Disconnect(ctx)

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, "", fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	var dbName string
	if db.ConnectionURI != "" {
		parts := strings.Split(db.ConnectionURI, "/")
		if len(parts) > 3 {
			dbNameParts := strings.Split(parts[len(parts)-1], "?")
			dbName = dbNameParts[0]
		}
	}

	if dbName == "" {
		dbName = db.DatabaseName
	}

	database := client.Database(dbName)
	return executeMongoDBGoCode(database, query, ctx, startTime)
}

// executeMongoDBGoCode executes MongoDB queries from Go code generated by AI
func executeMongoDBGoCode(database *mongo.Database, code string, ctx context.Context, startTime time.Time) ([]QueryResult, string, error) {
	fmt.Printf("Executing MongoDB Go code:\n%s\n", code)

	// Extract collection name
	collectionRegex := regexp.MustCompile(`var collection = "([^"]+)"`)
	collectionMatch := collectionRegex.FindStringSubmatch(code)
	if len(collectionMatch) < 2 {
		return nil, "", fmt.Errorf("missing collection name in generated code")
	}
	collectionName := collectionMatch[1]

	// Extract operation type
	operationRegex := regexp.MustCompile(`var operation = "([^"]+)"`)
	operationMatch := operationRegex.FindStringSubmatch(code)
	if len(operationMatch) < 2 {
		return nil, "", fmt.Errorf("missing operation type in generated code")
	}
	operationType := operationMatch[1]

	var filter bson.M
	var findOptions *options.FindOptions
	var pipeline mongo.Pipeline

	if operationType == "find" {
		// Extract filter
		filterRegex := regexp.MustCompile(`\*FILTER_START([\s\S]*?)\*FILTER_END`)
		filterMatch := filterRegex.FindStringSubmatch(code)
		if len(filterMatch) >= 2 {
			filterContent := strings.TrimSpace(filterMatch[1])
			if strings.HasPrefix(filterContent, "bson.M{") {
				filterContent = strings.TrimPrefix(filterContent, "bson.M{")
				filterContent = strings.TrimSuffix(filterContent, "}")
				if filterContent != "" {
					f, err := parseBSONM(filterContent)
					if err == nil {
						filter = f
					} else {
						fmt.Printf("Error parsing filter: %v\n", err)
					}
				}
			}
		}

		// Initialize findOptions
		findOptions = options.Find()

		// Extract sort
		sortRegex := regexp.MustCompile(`\*SORT_START([\s\S]*?)\*SORT_END`)
		sortMatch := sortRegex.FindStringSubmatch(code)
		if len(sortMatch) >= 2 {
			sortContent := strings.TrimSpace(sortMatch[1])
			if strings.HasPrefix(sortContent, "bson.D{") {
				sortContent = strings.TrimPrefix(sortContent, "bson.D{")
				sortContent = strings.TrimSuffix(sortContent, "}")
				sort, err := parseBSOND(sortContent)
				if err == nil {
					findOptions.SetSort(sort)
				} else {
					fmt.Printf("Error parsing sort: %v\n", err)
				}
			}
		}

		// Extract limit
		limitRegex := regexp.MustCompile(`\*LIMIT_START([\s\S]*?)\*LIMIT_END`)
		limitMatch := limitRegex.FindStringSubmatch(code)
		if len(limitMatch) >= 2 {
			limitContent := strings.TrimSpace(limitMatch[1])
			if limit, err := strconv.ParseInt(limitContent, 10, 64); err == nil {
				findOptions.SetLimit(limit)
			} else {
				fmt.Printf("Error parsing limit: %v\n", err)
			}
		}

		// Extract projection
		projRegex := regexp.MustCompile(`\*PROJECTION_START([\s\S]*?)\*PROJECTION_END`)
		projMatch := projRegex.FindStringSubmatch(code)
		if len(projMatch) >= 2 {
			projContent := strings.TrimSpace(projMatch[1])
			if strings.HasPrefix(projContent, "bson.D{") {
				projContent = strings.TrimPrefix(projContent, "bson.D{")
				projContent = strings.TrimSuffix(projContent, "}")
				proj, err := parseBSOND(projContent)
				if err == nil {
					findOptions.SetProjection(proj)
				} else {
					fmt.Printf("Error parsing projection: %v\n", err)
				}
			}
		}
	} else if operationType == "aggregate" {
		// Extract pipeline
		pipelineRegex := regexp.MustCompile(`\*PIPELINE_START([\s\S]*?)\*PIPELINE_END`)
		pipelineMatch := pipelineRegex.FindStringSubmatch(code)
		if len(pipelineMatch) >= 2 {
			pipelineContent := strings.TrimSpace(pipelineMatch[1])
			pipelineContent = strings.TrimPrefix(pipelineContent, "mongo.Pipeline{")
			pipelineContent = strings.TrimSuffix(pipelineContent, "}")
			if pipelineContent != "" {
				stages := splitPipelineStages(pipelineContent)
				for _, stage := range stages {
					stageContent := strings.TrimSpace(stage)
					if strings.HasPrefix(stageContent, "bson.D{") {
						stageContent = strings.TrimPrefix(stageContent, "bson.D{")
						stageContent = strings.TrimSuffix(stageContent, "}")
						s, err := parseBSOND(stageContent)
						if err == nil {
							pipeline = append(pipeline, s)
						} else {
							fmt.Printf("Error parsing pipeline stage: %v\n", err)
						}
					}
				}
			}
		}
	} else {
		return nil, "", fmt.Errorf("unsupported MongoDB operation: %s", operationType)
	}

	var results []bson.M

	if operationType == "find" {
		if filter == nil {
			filter = bson.M{}
		}
		if findOptions == nil {
			findOptions = options.Find()
		}

		fmt.Printf("Executing find on collection '%s' with filter: %+v, options: %+v\n", collectionName, filter, findOptions)
		cursor, err := database.Collection(collectionName).Find(ctx, filter, findOptions)
		if err != nil {
			return nil, "", fmt.Errorf("failed to execute find query: %v", err)
		}
		defer cursor.Close(ctx)

		if err := cursor.All(ctx, &results); err != nil {
			return nil, "", fmt.Errorf("failed to decode results: %v", err)
		}
	} else if operationType == "aggregate" {
		if len(pipeline) == 0 {
			pipeline = mongo.Pipeline{
				bson.D{{Key: "$match", Value: bson.M{}}},
				bson.D{{Key: "$limit", Value: 100}},
			}
		}

		fmt.Printf("Executing aggregate on collection '%s' with pipeline: %+v\n", collectionName, pipeline)
		cursor, err := database.Collection(collectionName).Aggregate(ctx, pipeline)
		if err != nil {
			return nil, "", fmt.Errorf("failed to execute aggregate query: %v", err)
		}
		defer cursor.Close(ctx)

		if err := cursor.All(ctx, &results); err != nil {
			return nil, "", fmt.Errorf("failed to decode results: %v", err)
		}
	}

	queryResults := make([]QueryResult, len(results))
	for i, result := range results {
		queryResult := make(QueryResult)
		for key, value := range result {
			queryResult[key] = sanitizeValue(value)
		}
		queryResults[i] = queryResult
	}

	executionTime := time.Since(startTime).String()
	return queryResults, executionTime, nil
}

// parseBSONM parses a bson.M string into a bson.M map, handling dot notation
func parseBSONM(content string) (bson.M, error) {
	result := bson.M{}
	content = strings.TrimSpace(strings.TrimSuffix(content, ","))
	if content == "" {
		return result, nil
	}

	pairs := splitBSONPairs(content)
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.Trim(strings.TrimSpace(parts[0]), `"`)
		valueStr := strings.TrimSpace(parts[1])

		if strings.HasPrefix(valueStr, "bson.M{") {
			nestedContent := strings.TrimPrefix(valueStr, "bson.M{")
			nestedContent = strings.TrimSuffix(nestedContent, "}")
			nested, err := parseBSONM(nestedContent)
			if err != nil {
				return nil, fmt.Errorf("failed to parse nested bson.M: %v", err)
			}
			result[key] = nested
		} else if valueStr == "nil" {
			result[key] = nil
		} else {
			var value interface{}
			if strings.HasPrefix(valueStr, `"`) && strings.HasSuffix(valueStr, `"`) {
				value = strings.Trim(valueStr, `"`)
			} else if num, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
				value = num
			} else if num, err := strconv.ParseFloat(valueStr, 64); err == nil {
				value = num
			} else {
				value = valueStr
			}
			result[key] = value
		}
	}

	return result, nil
}

// parseBSOND parses a bson.D string into a bson.D slice
func parseBSOND(content string) (bson.D, error) {
	var result bson.D
	content = strings.TrimSpace(strings.TrimSuffix(content, ","))
	if content == "" {
		return result, nil
	}

	pairs := splitBSONPairs(content)
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if !strings.HasPrefix(pair, "{") || !strings.HasSuffix(pair, "}") {
			continue
		}
		pair = strings.TrimPrefix(pair, "{")
		pair = strings.TrimSuffix(pair, "}")

		parts := strings.SplitN(pair, ",", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.Trim(strings.TrimSpace(parts[0]), `"`)
		valueStr := strings.TrimSpace(parts[1])

		var value interface{}
		if strings.HasPrefix(valueStr, "bson.M{") {
			nestedContent := strings.TrimPrefix(valueStr, "bson.M{")
			nestedContent = strings.TrimSuffix(nestedContent, "}")
			nested, err := parseBSONM(nestedContent)
			if err != nil {
				return nil, fmt.Errorf("failed to parse bson.M in bson.D: %v", err)
			}
			value = nested
		} else if strings.HasPrefix(valueStr, `"`) && strings.HasSuffix(valueStr, `"`) {
			value = strings.Trim(valueStr, `"`)
		} else if valueStr == "nil" {
			value = nil
		} else if num, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
			value = int32(num) // MongoDB typically uses int32 for sort/projection values
		} else if num, err := strconv.ParseFloat(valueStr, 64); err == nil {
			value = num
		} else {
			return nil, fmt.Errorf("unsupported value type in bson.D: %s", valueStr)
		}

		result = append(result, bson.E{Key: key, Value: value})
	}

	return result, nil
}

// splitBSONPairs splits a bson.M or bson.D string into key-value pairs, respecting nested structures
func splitBSONPairs(content string) []string {
	var pairs []string
	var current strings.Builder
	depth := 0
	inQuotes := false

	for _, r := range content {
		if r == '"' {
			inQuotes = !inQuotes
		}
		if !inQuotes {
			if r == '{' {
				depth++
			} else if r == '}' {
				depth--
			} else if r == ',' && depth == 0 {
				pairs = append(pairs, current.String())
				current.Reset()
				continue
			}
		}
		current.WriteRune(r)
	}

	if current.String() != "" {
		pairs = append(pairs, current.String())
	}
	return pairs
}

// splitPipelineStages splits a pipeline string into individual stages
func splitPipelineStages(content string) []string {
	var stages []string
	var current strings.Builder
	depth := 0
	inQuotes := false

	for _, r := range content {
		if r == '"' {
			inQuotes = !inQuotes
		}
		if !inQuotes {
			if r == '{' {
				depth++
			} else if r == '}' {
				depth--
			} else if r == ',' && depth == 0 {
				stages = append(stages, current.String())
				current.Reset()
				continue
			}
		}
		current.WriteRune(r)
	}

	if current.String() != "" {
		stages = append(stages, current.String())
	}
	return stages
}

// sanitizeValue handles special values like NaN and Infinity that can't be serialized to JSON
func sanitizeValue(value interface{}) interface{} {
	if f, ok := value.(float64); ok {
		if math.IsNaN(f) {
			return "NaN"
		}
		if math.IsInf(f, 1) {
			return "Infinity"
		}
		if math.IsInf(f, -1) {
			return "-Infinity"
		}
	}

	if m, ok := value.(map[string]interface{}); ok {
		result := make(map[string]interface{})
		for k, v := range m {
			result[k] = sanitizeValue(v)
		}
		return result
	}

	if s, ok := value.([]interface{}); ok {
		result := make([]interface{}, len(s))
		for i, v := range s {
			result[i] = sanitizeValue(v)
		}
		return result
	}

	return value
}
