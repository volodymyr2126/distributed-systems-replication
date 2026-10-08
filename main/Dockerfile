FROM python:3.14-alpine
RUN apk add --no-cache curl ca-certificates
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh
ENV PATH="/app/.venv/bin:/root/.local/bin:$PATH"
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY main/ /app/code/
COPY common/ /app/code/common/
RUN uv sync --locked --no-dev --no-install-project
CMD ["fastapi", "run", "code/src/main.py", "--port", "80"]