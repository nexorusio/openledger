FROM python:3.11-slim AS base
LABEL maintainer="Soxoj <soxoj@protonmail.com>"
WORKDIR /app
RUN pip install --no-cache-dir --upgrade pip
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
      build-essential \
      python3-dev \
      pkg-config \
      libcairo2-dev \
      libxml2-dev \
      libxslt1-dev \
    && rm -rf /var/lib/apt/lists/* /tmp/*
COPY . .
RUN YARL_NO_EXTENSIONS=1 python3 -m pip install --no-cache-dir .
# For production use, set FLASK_HOST to a specific IP address for security
ENV FLASK_HOST=0.0.0.0

# Web UI variant: a supervised production WSGI server on $PORT. A single
# process preserves the current in-memory live-job/SSE coordination, while
# threads allow concurrent status, report, and streaming requests.
FROM base AS web
RUN pip install --no-cache-dir '.[pdf]' 'gunicorn>=23,<24'
ENV PORT=5000
ENV GUNICORN_THREADS=4
ENV GUNICORN_TIMEOUT=600
EXPOSE 5000
ENTRYPOINT ["sh", "-c", "exec gunicorn --bind \"0.0.0.0:${PORT:-5000}\" --workers 1 --worker-class gthread --threads \"${GUNICORN_THREADS:-4}\" --timeout \"${GUNICORN_TIMEOUT:-600}\" --graceful-timeout 30 --keep-alive 5 --access-logfile - --error-logfile - maigret.web.app:app"]

# Default variant (last stage = `docker build .` target): CLI, backwards-compatible
FROM base AS cli
ENTRYPOINT ["maigret"]
