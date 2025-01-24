# Bluesky Firehose
Bluesky is a decentralized social media application. This project aims to explore Bluesky's data and process it for both real-time and historical analytics. It will levearge Bluesky's Firehose API, which offers a high volume data feed of all Bluesky posts. 

## Architecture

### Backend
- FastAPI backend with uvicorn/guvicorn
- Kafka messaging queue to handle high volume data
- Postgresql database
- Docker for containerization

### Frontend
- React.js frontend with Typescript
- Visualization (TBD)
