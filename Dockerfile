# Domo → Snowflake migration tools
# Build:  docker compose build
# Run:    docker compose run --rm cli <command> [options]
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=10

WORKDIR /app

# git is occasionally needed by transitive build steps; kept minimal otherwise.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# 1) Install the local argo-utils-cli clone (provides the `domo_utils` package).
#    It must be cloned next to this Dockerfile first:
#      git clone https://github.com/CrystalBallers/argo-utils-cli.git argo-utils-cli
COPY argo-utils-cli/ ./argo-utils-cli/
RUN pip install --upgrade pip && pip install -e ./argo-utils-cli

# 2) Install project dependencies (cached layer; only re-runs when requirements change).
COPY requirements.txt ./
RUN pip install -r requirements.txt

# 3) Copy the application code last so source edits don't bust the dependency cache.
COPY . .

# `docker compose run --rm cli inventory --test-connection` ->
#   python main.py inventory --test-connection
ENTRYPOINT ["python", "main.py"]
CMD ["--help"]
