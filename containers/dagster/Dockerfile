FROM python:3.10-slim

COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /bin/

# install Python dependencies
RUN --mount=type=cache,target=/root/.cache/uv uv pip install --system dagster dagster-webserver

WORKDIR /opt/dagster/app

COPY . /opt/dagster/app/

EXPOSE 3000
ENV DAGSTER_HOME=/dagster_home
CMD ["dagster", "dev", "-m", "blake", "--host", "0.0.0.0", "--port", "3000"]
#CMD ["dagster", "dev", "--host", "0.0.0.0", "--port", "3000"]
