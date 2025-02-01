FROM jupyter/base-notebook:python-3.9

RUN pip install duckdb minio pandas numpy matplotlib

# Install DuckDB's httpfs extension
RUN python -c "import duckdb; conn = duckdb.connect(); conn.install_extension('httpfs'); conn.load_extension('httpfs');"
