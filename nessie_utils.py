import duckdb
import sqlalchemy as sa
from urllib.parse import quote_plus
from config import POSTGRES_CONFIG, NESSIE_CONFIG


def push_df_to_nessie(df, pg_table_name, trino_table_name,
                      pg_user=None, pg_password=None,
                      pg_host=None, pg_port=None, pg_db=None,
                      trino_user=None, trino_host=None, trino_port=None,
                      trino_catalog=None, trino_schema=None):
    """
    Fonction pour envoyer un DataFrame vers Postgres via DuckDB et créer une table CETAS dans Trino/Nessie.
    
    Args:
        df: DataFrame à envoyer
        pg_table_name: Nom de la table dans Postgres
        trino_table_name: Nom de la table dans Trino/Nessie
        Autres paramètres: Configuration (utilise config.py si non spécifiés)
    """
    # Utiliser les valeurs par défaut du fichier de configuration
    pg_user = pg_user or POSTGRES_CONFIG['user']
    pg_password = pg_password or POSTGRES_CONFIG['password']
    pg_host = pg_host or POSTGRES_CONFIG['host']
    pg_port = pg_port or POSTGRES_CONFIG['port']
    pg_db = pg_db or POSTGRES_CONFIG['database']
    
    trino_user = trino_user or NESSIE_CONFIG['trino_user']
    trino_host = trino_host or NESSIE_CONFIG['trino_host']
    trino_port = trino_port or NESSIE_CONFIG['trino_port']
    trino_catalog = trino_catalog or NESSIE_CONFIG['trino_catalog']
    trino_schema = trino_schema or NESSIE_CONFIG['trino_schema']
    
    # ==================================================
    # 1. Connexion DuckDB (in-memory)
    # ==================================================
    con = duckdb.connect()
    con.register("df_local", df)
    
    # Encodage du mot de passe Postgres
    encoded_password = quote_plus(pg_password)
    pg_conn_str = f"{pg_user}:{encoded_password}@{pg_host}:{pg_port}/{pg_db}"
    
    # Installer et charger l'extension PostgreSQL
    con.execute("""
    INSTALL postgres;
    LOAD postgres;
    """)
    
    # Attacher Postgres
    con.execute(f"ATTACH 'postgresql://{pg_conn_str}' AS pg (TYPE POSTGRES);")
    
    # Créer ou remplacer la table dans Postgres
    con.execute(f"CREATE OR REPLACE TABLE pg.{pg_table_name} AS SELECT * FROM df_local;")
    print(f"✅ Table '{pg_table_name}' créée dans Postgres")
    
    # Vérification
    result = con.execute(f"SELECT COUNT(*) FROM pg.{pg_table_name}").fetchone()
    print(f"✅ Nombre de lignes dans la table Postgres : {result[0]}")
    
    # ==================================================
    # 2. Connexion Trino
    # ==================================================
    trino_engine = sa.create_engine(f"trino://{trino_user}@{trino_host}:{trino_port}?auth=none")
    
    # Création de la table CETAS dans Trino/Nessie
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
    
    # Vérification
    try:
        with trino_engine.connect() as conn:
            result = conn.execute(sa.text(f'SELECT * FROM "{trino_catalog}"."{trino_schema}"."{trino_table_name}" LIMIT 5'))
            print("✅ Premières lignes de la table CETAS :")
            for row in result:
                print(row)
    except Exception as e:
        print(f"❌ Erreur lors de la vérification Trino/Nessie: {e}")
