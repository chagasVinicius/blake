FROM jupyter/base-notebook:python-3.10
RUN pip install duckdb==1.1.3 minio pandas numpy matplotlib
RUN python -c "import duckdb; conn = duckdb.connect(); conn.install_extension('httpfs'); conn.load_extension('httpfs');"
