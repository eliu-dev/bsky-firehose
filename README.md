# Bluesky Data Explorer
Bluesky is a decentralized social media application. This project aims to explore Bluesky's data and process it for both real-time analytics and historical analytics. It will leverage Bluesky's Firehose API via [Jetstream][https://github.com/bluesky-social/jetstream], which offers a high volume data feed of all Bluesky posts. 

## Architecture

### Backend
- FastAPI backend with uvicorn/guvicorn
- Kafka messaging queue to handle high volume data
- Postgresql database
- Docker for containerization

### Frontend
- React.js frontend with Typescript
- Visualization (TBD)

## Project structure
├── docker-compose.yml
├── .env.example              # Template for environment variables
├── backend/
│   ├── __init__.py
│   ├── Dockerfile            
│   ├── requirements.txt      
│   └── app/
│       ├── main.py           # FastAPI application entry point
│       ├── core/             
│       │   ├── __init__.py
│       │   ├── config.py     # Configuration management for services
│       │   └── database.py
│       ├── models/
│       │   ├── __init__.py
│       │   └── jetstream.py  # Jetstream message model
│       ├── services/         # Business logic (where the cooking happens)
│       │   ├── __init__.py
│       │   ├── jetstream.py  
│       │   └── kafka.py      
│       └── workers/          
│           ├── __init__.py
│           ├── ingest.py     # Processes data from Jetstream
│           └── process.py    # Processes data from Kafka
├── frontend/                 # React frontend
│   └── ...
└── shared/                   # Shared types and utilities
    └── ...