# nessie_utils.py
import duckdb
from urllib.parse import quote_plus
import sqlalchemy as sa

def push_df_to_nessie(df, pg_table_name, trino_table_name,
                      pg_user="postgres", pg_password="postgres",
                      pg_host="10.10.20.36", pg_port=30894, pg_db="postgres_cie",
                      trino_user="admin", trino_host="10.10.20.36", trino_port=30808,
                      trino_catalog="minio-test", trino_schema="sime-dwh"):
    """
    Fonction pour envoyer un DataFrame vers Postgres via DuckDB et créer une table CETAS dans Trino/Nessie.
    """
    # Connexion DuckDB (in-memory)
    con = duckdb.connect()
    con.register("df_local", df)
    
    encoded_password = quote_plus(pg_password)
    pg_conn_str = f"{pg_user}:{encoded_password}@{pg_host}:{pg_port}/{pg_db}"
    
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH 'postgresql://{pg_conn_str}' AS pg (TYPE POSTGRES);")
    con.execute(f"CREATE OR REPLACE TABLE pg.{pg_table_name} AS SELECT * FROM df_local;")
    print(f"✅ Table '{pg_table_name}' créée dans Postgres")
    
    result = con.execute(f"SELECT COUNT(*) FROM pg.{pg_table_name}").fetchone()
    print(f"✅ Nombre de lignes dans la table Postgres : {result[0]}")
    
    # Connexion Trino
    trino_engine = sa.create_engine(f"trino://{trino_user}@{trino_host}:{trino_port}?auth=none")
    
    cetas_query = f"""
    CREATE OR REPLACE TABLE "{trino_catalog}"."{trino_schema}"."{trino_table_name}" AS
    SELECT * FROM "{pg_db}".public.{pg_table_name}
    """
    try:
        with trino_engine.connect() as conn:
            conn.execute(sa.text(cetas_query))
        print(f"✅ Table '{trino_table_name}' créée dans Nessie via CETAS")
    except Exception as e:
        print(f"❌ Erreur lors de la création de la table Trino/Nessie: {e}")
    
    try:
        with trino_engine.connect() as conn:
            result = conn.execute(sa.text(f'SELECT * FROM "{trino_catalog}"."{trino_schema}"."{trino_table_name}" LIMIT 5'))
            print("✅ Premières lignes de la table CETAS :")
            for row in result:
                print(row)
    except Exception as e:
        print(f"❌ Erreur lors de la vérification Trino/Nessie: {e}")
