import sqlalchemy as sa
from urllib.parse import quote_plus
from config import POSTGRES_CONFIG, NESSIE_CONFIG


def push_df_to_nessie(df, pg_table_name, trino_table_name,
                      pg_user=None, pg_password=None,
                      pg_host=None, pg_port=None, pg_db=None,
                      trino_user=None, trino_host=None, trino_port=None,
                      trino_catalog=None, trino_schema=None):
    """
    Envoie un DataFrame vers Postgres via SQLAlchemy/psycopg2
    puis crée une table CETAS dans Trino/Nessie.
    """

    # Valeurs par défaut depuis la config
    pg_user     = pg_user     or POSTGRES_CONFIG['user']
    pg_password = pg_password or POSTGRES_CONFIG['password']
    pg_host     = pg_host     or POSTGRES_CONFIG['host']
    pg_port     = pg_port     or POSTGRES_CONFIG['port']
    pg_db       = pg_db       or POSTGRES_CONFIG['database']

    trino_user    = trino_user    or NESSIE_CONFIG['trino_user']
    trino_host    = trino_host    or NESSIE_CONFIG['trino_host']
    trino_port    = trino_port    or NESSIE_CONFIG['trino_port']
    trino_catalog = trino_catalog or NESSIE_CONFIG['trino_catalog']
    trino_schema  = trino_schema  or NESSIE_CONFIG['trino_schema']

    # ==================================================
    # 1. Écriture dans Postgres via SQLAlchemy + psycopg2
    # ==================================================
    encoded_password = quote_plus(pg_password)
    pg_engine = sa.create_engine(
        f"postgresql+psycopg2://{pg_user}:{encoded_password}@{pg_host}:{pg_port}/{pg_db}",
        pool_pre_ping=True   # vérifie la connexion avant usage
    )

    try:
        df.to_sql(
            name=pg_table_name,
            con=pg_engine,
            schema="public",
            if_exists="replace",   # équivalent de CREATE OR REPLACE
            index=False,
            method="multi",        # INSERT multi-lignes, plus rapide
            chunksize=5000
        )
        print(f"✅ Table '{pg_table_name}' créée dans Postgres ({len(df)} lignes)")
    except Exception as e:
        print(f"❌ Erreur lors de l'écriture Postgres : {e}")
        raise   # on arrête ici, pas la peine de continuer vers Trino
    finally:
        pg_engine.dispose()

    # ==================================================
    # 2. Création de la table CETAS dans Trino/Nessie
    # ==================================================
    trino_engine = sa.create_engine(
        f"trino://{trino_user}@{trino_host}:{trino_port}?auth=none"
    )

    cetas_query = f"""
    CREATE OR REPLACE TABLE "{trino_catalog}"."{trino_schema}"."{trino_table_name}" AS
    SELECT * FROM "{pg_db}".public.{pg_table_name}
    """

    try:
        with trino_engine.connect() as conn:
            conn.execute(sa.text(cetas_query))
        print(f"✅ Table '{trino_table_name}' créée dans Nessie via CETAS")
    except Exception as e:
        print(f"❌ Erreur lors de la création Trino/Nessie : {e}")
        raise
    finally:
        trino_engine.dispose()

    # Vérification : affiche les 5 premières lignes
    trino_engine = sa.create_engine(
        f"trino://{trino_user}@{trino_host}:{trino_port}?auth=none"
    )
    try:
        with trino_engine.connect() as conn:
            result = conn.execute(
                sa.text(f'SELECT * FROM "{trino_catalog}"."{trino_schema}"."{trino_table_name}" LIMIT 5')
            )
            print("✅ Premières lignes de la table CETAS :")
            for row in result:
                print(row)
    except Exception as e:
        print(f"❌ Erreur lors de la vérification Trino/Nessie : {e}")
    finally:
        trino_engine.dispose()
