# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Personal Investment Machine (PIM) backend service built with FastAPI and PostgreSQL. The goal is to collect Korean stock market data and provide investment screening capabilities.

**Current Stage**: Collecting Korean stock market company names and stock prices for investment candidate filtering.

## Development Setup

### Using Docker (Recommended)
```bash
# Start the full stack (PostgreSQL + FastAPI)
docker-compose up --build

# Backend will be available at http://localhost:8000
# PostgreSQL will be available at localhost:5432
```

### Local Development
```bash
# Install dependencies using uv
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e .

# Run database migrations
alembic upgrade head

# Start the development server
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Architecture

### Directory Structure
- `app/` - Main application package
  - `main.py` - FastAPI application entry point
  - `db/` - Database configuration and models
    - `session.py` - SQLAlchemy session and engine setup
    - `base.py` - Base model imports for Alembic
  - `services/` - Business logic and external integrations
    - `stock_historical_data.py` - Korean stock market data collection using pykrx

### Key Technologies
- **FastAPI** - Web framework
- **SQLAlchemy** - ORM for database operations
- **Alembic** - Database migrations
- **PostgreSQL** - Primary database
- **pykrx** - Korean stock market data source
- **uv** - Python package management

### Database
- Uses PostgreSQL with SQLAlchemy ORM
- Database configuration imported from `app.core.config.settings` (missing - needs implementation)
- Migrations managed through Alembic
- Connection string expected via `settings.DATABASE_URL`

## Common Commands

### Database Operations
```bash
# Create new migration
alembic revision --autogenerate -m "description"

# Apply migrations
alembic upgrade head

# Rollback migration
alembic downgrade -1
```

### Docker Operations
```bash
# Rebuild and start services
docker-compose up --build

# View logs
docker-compose logs backend
docker-compose logs db

# Stop services
docker-compose down
```

## Missing Components

The following components are referenced but not yet implemented:
- `app.core.config` module for settings management
- Environment configuration (.env file structure)
- Database models/schemas
- API endpoints in main.py
- Alembic configuration and migrations directory

## Stock Data Integration

The project uses `pykrx` library to collect Korean stock market data:
- KOSPI and KOSDAQ market ticker lists
- Combines both markets for comprehensive coverage
- Located in `app/services/stock_historical_data.py`