# Spatial Data Search

An application that uses Large Language Models (LLMs) to enhance spatial data discovery in Spatial Data Infrastructures (SDIs). It implements a Retrieval-Augmented Generation (RAG) workflow that utilizes spatial data metadata. It is meant to overcome the limitations of current search mechanisms in SDIs, such as rigid keyword-based search, complex user interfaces, and language barriers. Users are able to search for spatial data using natural language.

## Features
- 🌍 **Geocoding** - Converting the location names in user queries to their bounding boxes.
- 🌍 **Spatial Indexing** - PostGIS-based spatial data indexing that identifies datasets that meet the spatial intent of the user.
- 🔍 **Vector Search** - Semantic search using Qdrant vector store and OpenAI embeddings
- 🤖 **LLM-Powered Responses** - Natural language responses using an LLM.
- 🗺️ **Multilingual Support** - Users can search for spatial data using any language, regardless of the language used in the metadata.
- 📊 **DCAT Metadata Harvesting** - Fetch and index datasets from European Data Portal and local RDF files
- 🎯 **Hybrid Search** - Combine spatial and semantic search for precise results


## Architecture

## Prerequisites
- **Python** 3.9+
- **Docker & Docker Compose** (for PostgreSQL and Qdrant)
- **OpenAI API Key** (for embeddings and LLM)
- **PostgreSQL** with PostGIS extension (via Docker)
- **Qdrant** vector database (via Docker)

## Quick Start
### 1. Install uv (if not already installed)
```bash
# With pip.
pip install uv

# Or pipx.
pipx install uv

# Verify installation
uv --version
```

### 2. Clone and Setup Environment
Clone the project repository
```bash
git clone https://github.com/JamesOkemwa/SDI_data_search_LLM.git
```

Create virtual environment and install dependencies
```bash
cd .\spatial_data_search\
uv sync
```

**What `uv sync` does:**
- Creates `.venv/` virtual environment
- Installs all dependencies from `pyproject.toml`
- Locks versions in `uv.lock` for reproducibility

### 3. Configure Environment Variables
Create a `.env` file in the project root: Apart from the OPENAI_API_KEY, the default env values can be found in the env.sample file.

```env
# OpenAI Configuration
OPENAI_API_KEY=

# PostgreSQL Configuration
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_HOST=
POSTGRES_PORT=
POSTGRES_DB=

# Qdrant Configuration
QDRANT_HOST=
QDRANT_PORT=
```

### 4. Start the required services
Start PostgreSQL and Qdrant via Docker
```bash
docker-compose up -d
```

Verify services are running
```bash
docker-compose ps
```

### 5. Harvest & Index datasets
Initialize database and harvest datasets
```bash
uv run metadata_harvester.py
```

This will:
- Create PostGIS schema and spatial indexes
- Harvest metadata from the European Data Portal
- Index datasets in both PostGIS and Qdrant
- Harvest metadata from local RDF files

### 6. Run the application
Start the backend service
```bash
uv run uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Start the frontend application
```bash
cd .\frontend\

uv run streamlit run .\app.py
``` 