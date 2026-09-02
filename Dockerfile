FROM python:3.11-slim AS base
LABEL maintainer="Soxoj <soxoj@protonmail.com>"
WORKDIR /app
RUN pip install --no-cache-dir --upgrade pip
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
      build-essential \
      fonts-dejavu-core \
      fonts-droid-fallback \
      python3-dev \
      pkg-config \
      libcairo2-dev \
      libxml2-dev \
      libxslt1-dev \
    && rm -rf /var/lib/apt/lists/* /tmp/*
COPY . .
RUN YARL_NO_EXTENSIONS=1 python3 -m pip install --no-cache-dir .
RUN groupadd --gid 10001 openledger && \
    useradd --uid 10001 --gid 10001 --no-create-home \
      --home-dir /nonexistent --shell /usr/sbin/nologin openledger
RUN install -d -m 0700 -o 10001 -g 10001 /app/runtime/secrets
# For production use, set FLASK_HOST to a specific IP address for security
ENV FLASK_HOST=0.0.0.0

# Web UI variant: a supervised production WSGI server on $PORT. Durable jobs
# run in the separate worker service; threads allow concurrent page, report,
# and replayable streaming requests.
FROM base AS web
# Keep the optional web collector isolated from OpenLedger's declared Python
# dependency graph and pin it to a safety-reviewed commit. Upgrades happen only
# through a reviewed pin change, never by following a moving branch at build.
ARG USER_SCANNER_ARCHIVE_URL="https://github.com/nexorusio/user-scanner/archive/6fcd9d677eb25601268ebab5bfb4ed27e00e1157.tar.gz"
RUN python3 -m pip install --no-cache-dir "${USER_SCANNER_ARCHIVE_URL}"
# Unfurl's maintained release requires Python 3.11+ and NetworkX 3, while Maigret
# still supports Python 3.10 and NetworkX 2. Keep it in an immutable, dedicated
# environment and cross the boundary only through the bounded JSON runner.
ARG UNFURL_ARCHIVE_URL="https://github.com/RyanDFIR/unfurl/archive/a21ef7ce1896bd8db17aeeb990911877ab839dbe.tar.gz"
RUN python3 -m venv /opt/openledger-unfurl && \
    /opt/openledger-unfurl/bin/python -m pip install --no-cache-dir --upgrade pip && \
    /opt/openledger-unfurl/bin/python -m pip install --no-cache-dir "${UNFURL_ARCHIVE_URL}"
RUN pip install --no-cache-dir '.[pdf]' 'gunicorn>=23,<24'
ENV PORT=5000
ENV GUNICORN_THREADS=4
ENV GUNICORN_TIMEOUT=600
EXPOSE 5000
USER 10001:10001
ENTRYPOINT ["sh", "-c", "exec gunicorn --bind \"0.0.0.0:${PORT:-5000}\" --workers 1 --worker-class gthread --threads \"${GUNICORN_THREADS:-4}\" --timeout \"${GUNICORN_TIMEOUT:-600}\" --graceful-timeout 30 --keep-alive 5 --access-logfile - --error-logfile - maigret.web.app:app"]

# Default variant (last stage = `docker build .` target): CLI, backwards-compatible
FROM base AS cli
ENTRYPOINT ["maigret"]
