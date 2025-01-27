# bsky
Bluesky firehose data experiments

## Project structure
├── docker-compose.yml
├── .env.example              # Template for environment variables
├── backend/
│   ├── Dockerfile            # Instructions to build backend container
│   ├── requirements.txt      # Python dependencies
│   └── app/
│       ├── main.py           # FastAPI application entry point
│       ├── core/             # Core functionality (like a kitchen's foundations)
│       │   ├── __init__.py
│       │   ├── config.py     # Configuration management
│       │   └── database.py   # Database connection handling
│       ├── models/           # Database models (our recipe book)
│       │   ├── __init__.py
│       │   └── jetstream.py  # Jetstream message model
│       ├── services/         # Business logic (where the cooking happens)
│       │   ├── __init__.py
│       │   ├── jetstream.py   # Handles connection to Bluesky
│       │   └── kafka.py      # Manages message queue operations
│       └── workers/          # Background processes (like prep cooks)
│           ├── __init__.py
│           ├── ingest.py     # Receives data from Firehose
│           └── process.py    # Processes data from Kafka
├── frontend/                 # React frontend (stays the same)
│   └── ...
└── shared/                   # Shared types and utilities
    └── ...