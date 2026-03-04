# Use Python 3.13 as specified in your TOML
FROM python:3.13-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Enable bytecode compilation for faster startups
ENV UV_COMPILE_BYTECODE=1
# Ensure logs are sent straight to terminal
ENV PYTHONUNBUFFERED=1

# Copy project files
COPY pyproject.toml .
# If you have a lockfile, copy it to ensure exact versions
# COPY uv.lock .

# Install dependencies using the pyproject.toml
RUN uv pip install --system .

# Copy source code
COPY . .

# Run the FastStream app
CMD ["faststream", "run", "consumer:app", "--host", "0.0.0.0"]