# GoQuery Backend

This is the backend for the GoQuery application, built with Go, Gofiber, and MongoDB.

## Prerequisites

- Go 1.21 or higher
- MongoDB

## Getting Started

1. Clone the repository
2. Navigate to the backend directory
3. Install dependencies:

```bash
go mod tidy
```

4. Set up environment variables by creating a `.env` file (or use the existing one)
5. Run the server:

```bash
go run main.go
```

The server will start on port 8080 (or the port specified in your .env file).

## API Endpoints

### Authentication

- `POST /api/auth/signup` - Register a new user
  - Request body: `{ "email": "user@example.com", "password": "password", "name": "User Name" }`
  - Response: `{ "token": "jwt-token", "user": { ... } }`

- `POST /api/auth/login` - Login a user
  - Request body: `{ "email": "user@example.com", "password": "password" }`
  - Response: `{ "token": "jwt-token", "user": { ... } }`

- `GET /api/auth/me` - Get the current user
  - Headers: `Authorization: Bearer jwt-token`
  - Response: `{ "id": "...", "email": "user@example.com", "name": "User Name", ... }`

### Health Check

- `GET /health` - Check if the server is running
  - Response: `{ "status": "ok" }`

## Environment Variables

- `APP_PORT` - The port the server will run on (default: 8080)
- `APP_ENV` - The environment the server is running in (default: development)
- `MONGO_URI` - The MongoDB connection URI (default: mongodb://localhost:27017)
- `MONGO_DATABASE` - The MongoDB database name (default: goquery)
- `JWT_SECRET` - The secret key for JWT token generation
- `JWT_EXPIRY` - The expiry time for JWT tokens (default: 168h = 7 days)
- `ALLOW_ORIGINS` - CORS allowed origins (default: *)
