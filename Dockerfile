FROM python:3.12-slim

WORKDIR /app

# Install Python dependencies first (better layer caching)
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY backend/ ./backend/
COPY frontend/ ./backend/frontend/

# Run uvicorn from the backend directory so "main:app" and
# StaticFiles(directory="frontend") both resolve correctly
WORKDIR /app/backend

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
