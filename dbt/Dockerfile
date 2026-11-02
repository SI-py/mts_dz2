FROM python:3.12-slim@sha256:b18b261e1b4974b4f321451ad5251ded85abb22c2b8b262da527dda1d031ab30
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 DBT_PROFILES_DIR=/app/dbt DBT_SEND_ANONYMOUS_USAGE_STATS=false
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . ./dbt
COPY scripts ./scripts
WORKDIR /app/dbt
ENTRYPOINT ["dbt"]
CMD ["--help"]
