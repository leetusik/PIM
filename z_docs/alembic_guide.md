# Alembic Migration Guide

This guide covers how to use Alembic for database migrations in the PIM (Personal Investment Machine) project.

## Table of Contents
- [Overview](#overview)
- [Initial Setup](#initial-setup)
- [Creating Migrations](#creating-migrations)
- [Running Migrations](#running-migrations)
- [Common Commands](#common-commands)
- [Troubleshooting](#troubleshooting)

## Overview

Alembic is a database migration tool for SQLAlchemy. It allows you to:
- Track database schema changes over time
- Apply changes to databases in a controlled manner
- Roll back changes when needed
- Keep database schemas in sync across environments

## Initial Setup

### 1. Initialize Alembic (Already Done)

```bash
# This was already done in the project
alembic init alembic
```

### 2. Configuration Files

The project has the following Alembic configuration:

- `alembic.ini` - Main configuration file
- `alembic/env.py` - Environment setup script
- `alembic/versions/` - Directory containing migration files
- `app/db/base.py` - Imports all models for Alembic detection

### 3. Key Configuration Changes Made

**In `alembic/env.py`:**
```python
# Import the base that has all the models registered
from app.db.base import Base
target_metadata = Base.metadata

# Use environment variable for DATABASE_URL
import os
from sqlalchemy import create_engine

database_url = os.environ.get('DATABASE_URL')
if not database_url:
    raise ValueError("DATABASE_URL environment variable not set")

connectable = create_engine(database_url, poolclass=pool.NullPool)
```

**In `app/db/base.py`:**
```python
from app.db.session import Base

# Import all models here for Alembic auto-detection
from app.models.stock import Stock  # noqa: F401
```

## Creating Migrations

### Auto-generate Migration from Model Changes

```bash
# Using Docker (recommended)
docker-compose run --rm backend alembic revision --autogenerate -m "Description of changes"

# Local development (requires local PostgreSQL)
alembic revision --autogenerate -m "Description of changes"
```

### Manual Migration Creation

```bash
# Create empty migration file
docker-compose run --rm backend alembic revision -m "Description of changes"
```

### Migration File Structure

Generated migration files are located in `alembic/versions/` and look like:

```python
"""Initial migration with Stock model

Revision ID: 4e55a8b046a1
Revises: 
Create Date: 2025-08-16 08:26:13.620580
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '4e55a8b046a1'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('stocks',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('market', sa.String(), nullable=True),
        sa.Column('ticker', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_stocks_id'), 'stocks', ['id'], unique=False)

def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_stocks_id'), table_name='stocks')
    op.drop_table('stocks')
```

## Running Migrations

### Apply All Pending Migrations

```bash
# Using Docker (recommended)
docker-compose run --rm backend alembic upgrade head

# Local development
alembic upgrade head
```

### Apply Specific Migration

```bash
# Upgrade to specific revision
docker-compose run --rm backend alembic upgrade 4e55a8b046a1

# Upgrade one step forward
docker-compose run --rm backend alembic upgrade +1
```

### Rollback Migrations

```bash
# Rollback one migration
docker-compose run --rm backend alembic downgrade -1

# Rollback to specific revision
docker-compose run --rm backend alembic downgrade 4e55a8b046a1

# Rollback all migrations
docker-compose run --rm backend alembic downgrade base
```

## Common Commands

### Check Current Migration Status

```bash
# Show current revision
docker-compose run --rm backend alembic current

# Show migration history
docker-compose run --rm backend alembic history

# Show pending migrations
docker-compose run --rm backend alembic heads
```

### Database Schema Inspection

```bash
# Connect to PostgreSQL and inspect tables
docker-compose exec db psql -U user -d mydatabase

# List all tables
\dt

# Describe specific table
\d stocks

# Check alembic version table
SELECT * FROM alembic_version;
```

### Development Workflow

1. **Make Model Changes:**
   ```python
   # Example: Add new field to Stock model
   class Stock(Base):
       __tablename__ = "stocks"
       
       id = Column(Integer, primary_key=True, index=True)
       name = Column(String, index=True)
       market = Column(String, index=True)
       ticker = Column(String, index=True)
       # New field
       sector = Column(String, index=True)  # Add this
   ```

2. **Generate Migration:**
   ```bash
   docker-compose run --rm backend alembic revision --autogenerate -m "Add sector field to Stock model"
   ```

3. **Review Generated Migration:**
   - Check the generated file in `alembic/versions/`
   - Ensure the changes are correct
   - Modify if needed

4. **Apply Migration:**
   ```bash
   docker-compose run --rm backend alembic upgrade head
   ```

5. **Verify Changes:**
   ```bash
   docker-compose exec db psql -U user -d mydatabase -c "\d stocks"
   ```

## Troubleshooting

### Common Issues

**1. "Could not parse SQLAlchemy URL"**
- Ensure DATABASE_URL environment variable is set
- Check that the database service is running

**2. "No changes in schema detected"**
- Ensure models are imported in `app/db/base.py`
- Check that model changes are actually different

**3. "Target database is not up to date"**
- Run `alembic upgrade head` first
- Check current revision with `alembic current`

**4. "Can't locate revision identified by"**
- Check if migration files exist in `alembic/versions/`
- Ensure revision IDs are correct

### Environment Variables

Required environment variables (set in `.env`):
```bash
DATABASE_URL=postgresql+psycopg2://user:password@db:5432/mydatabase
```

### Automatic Migration on Startup

The `entrypoint.sh` script automatically runs migrations when starting the backend:

```bash
#!/bin/bash
echo "Applying database migrations..."
alembic upgrade head

echo "Starting server..."
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Best Practices

1. **Always review generated migrations** before applying them
2. **Test migrations on a copy** of production data
3. **Create meaningful migration messages** that describe the change
4. **Don't edit applied migrations** - create new ones instead
5. **Backup your database** before applying migrations in production
6. **Use transactions** for complex migrations when possible

### Quick Reference

```bash
# Start services
docker-compose up

# Create migration
docker-compose run --rm backend alembic revision --autogenerate -m "description"

# Apply migrations
docker-compose run --rm backend alembic upgrade head

# Check status
docker-compose run --rm backend alembic current

# Rollback
docker-compose run --rm backend alembic downgrade -1
```