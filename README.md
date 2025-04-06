# Bluesky Analytics

A data analytics platform for Bluesky, processing and analyzing the Bluesky Jetstream API data stream. This project ingests data from the Bluesky Firehose (via [Jetstream](https://github.com/bluesky-social/jetstream)), processes it through Kafka, and stores it in PostgreSQL for analytics and visualization.

## Architecture

### Backend

- FastAPI backend with uvicorn/gunicorn
- Kafka messaging queue to handle high volume data
- PostgreSQL database for persistent storage
- Docker for containerization and deployment

### Data Flow

1. **Ingestion**: The Jetstream client connects to Bluesky's Firehose and streams real-time data
2. **Processing**: Messages are pushed to Kafka for buffering and reliable processing
3. **Persistence**: A worker consumes from Kafka and stores data in PostgreSQL
4. **Analytics**: FastAPI endpoints expose the data for the frontend to query
5. **Visualization**: React frontend visualizes the data with interactive charts and filters

### Frontend

- React.js frontend with TypeScript
- Tailwind CSS for styling
- Recharts for data visualization

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.10+
- Node.js 18+ (for frontend)

### Environment Setup

1. Clone the repository

   ```bash
   git clone https://github.com/yourusername/bsky-firehose.git
   cd bsky-firehose
   ```

2. Copy the example environment file and modify as needed

   ```bash
   cd backend
   cp .env.example .env.development
   ```

3. Start the development environment using Docker Compose
   ```bash
   docker-compose up -d
   ```
   This will start PostgreSQL, Kafka, and the backend API in development mode.

### Running Tests

The project includes several test scripts to verify functionality:

```bash
# Test PostgreSQL connection
python -m app.cli db-test

# Initialize PostgreSQL database tables
python -m app.cli db-init

# Run full PostgreSQL testing suite
python -m app.cli postgres-test

# Test Kafka connection
python -m app.cli kafka-test
```

### Running Components Individually

You can run the individual components of the system:

```bash
# Run the Firehose ingest worker (streams data to Kafka)
python -m app.cli ingest

# Run the persistence worker (stores data from Kafka to PostgreSQL)
python -m app.cli persistence
```

## Database Schema

The PostgreSQL database includes the following tables:

### BlueskyUser

Stores user information from the Bluesky network.

- `id`: UUID primary key
- `did`: Decentralized Identifier (unique)
- `handle`: User handle (e.g., @user.bsky.social)
- `active`: Boolean indicating if the account is active
- `seq`: Sequence number from Bluesky
- `bsky_timestamp`: Timestamp from Bluesky
- `created_at`: Local creation time
- `updated_at`: Local update time

### BlueskyPost

Stores posts and replies from the Bluesky network.

- `id`: UUID primary key
- `cid`: Content Identifier
- `uri`: Resource URI (unique)
- `text`: Post content
- `langs`: Language tags array
- `record_type`: Type of record
- `bsky_created_at`: Creation time from Bluesky
- `rev`: Revision identifier
- `rkey`: Record key
- `collection`: Collection name
- `operation`: Operation type (create, update, delete)
- `user_id`: Foreign key to BlueskyUser
- `parent_cid`: Parent post CID (for replies)
- `parent_uri`: Parent post URI (for replies)
- `root_cid`: Root post CID (for nested replies)
- `root_uri`: Root post URI (for nested replies)
- `additional_data`: Additional structured data (JSONB)
- `created_at`: Local creation time
- `updated_at`: Local update time

### RawMessage

Stores raw messages from the Firehose for archival and debugging.

- `id`: UUID primary key
- `did`: Decentralized Identifier
- `time_us`: Timestamp in microseconds
- `kind`: Message kind (commit, identity, account)
- `raw_data`: Raw message content (JSONB)
- `processed`: Boolean indicating if the message has been processed
- `created_at`: Local creation time
- `updated_at`: Local update time

## Project Structure

```
├── backend/
│   ├── docker-compose.yml      # Docker Compose configuration
│   ├── .env.development        # Development environment variables
│   ├── .env.example            # Template for environment variables
│   ├── Dockerfile              # Docker configuration
│   ├── requirements.txt        # Python dependencies
│   └── app/
│       ├── main.py             # FastAPI application entry point
│       ├── cli.py              # Command-line interface
│       ├── test_postgres.py    # PostgreSQL testing script
│       ├── core/               # Core application components
│       │   ├── config.py       # Configuration management
│       │   ├── database.py     # Database connections
│       │   └── logging.py      # Logging configuration
│       ├── db/                 # Database management
│       │   └── init_db.py      # Database initialization
│       ├── models/             # Data models
│       │   ├── jetstream_types.py  # Firehose message models
│       │   └── db/             # Database models
│       │       ├── base.py     # Base model classes
│       │       └── bluesky.py  # Bluesky-specific models
│       ├── repositories/       # Data access layer
│       │   ├── base.py         # Base repository class
│       │   └── bluesky.py      # Bluesky repositories
│       ├── services/           # External service clients
│       │   ├── jetstream_client.py  # Bluesky Firehose client
│       │   ├── kafka_client.py      # Kafka client
│       │   └── db_test.py      # Database testing utilities
│       └── workers/            # Background workers
│           ├── ingest.py       # Firehose ingestion worker
│           └── persistence.py  # Kafka to PostgreSQL worker
├── frontend/                   # React frontend
│   └── ...
```

## API Endpoints

The FastAPI backend exposes the following endpoints:

- `GET /`: Health check
- `GET /health`: Detailed health check
- `GET /jetstream-test`: Initiate Jetstream connection
- `GET /db-test`: Test database connection
- `GET /api/v1/posts`: Get recent posts
- `GET /api/v1/users`: Get users

## Development Workflow

1. Start the development environment with Docker Compose
2. Make changes to the code
3. FastAPI's hot reloading will automatically apply changes
4. Test changes using the CLI or API endpoints

## Deployment

For production deployment:

1. Set up production environment variables in `.env.production`
2. Build the Docker image: `docker build -t bsky-analytics .`
3. Deploy to your cloud provider or server using Docker Compose or Kubernetes
