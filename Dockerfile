# Use Python 3.13 slim for a small, fast image
FROM python:3.13-slim

# Install uv from the official binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

#Optimizations for faster builds and smaller image size 
#Stop Python from buffering logs
#Prevent Python from writing .pyc files in the container
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1

# Copy dependency files first to leverage Docker layer caching
COPY pyproject.toml uv.lock* ./

# Install dependencies globally in the container (standard for Docker)
# To keep the image size small
RUN uv pip install --system --no-cache -r pyproject.toml

# Copy the rest of the application code
COPY . .

# Create the data directory in case it's missing (helps producer not crash)
RUN mkdir -p /app/data

# Default command for the Consumer (overridden for the Producer in docker-compose)
# use 'python -m faststream' to ensure the path is correctly picked up
CMD ["faststream", "run", "consumer:app"]