from trino.dbapi import connect
import sqlalchemy as sa
# Configuration Trino
TRINO_CONFIG = {
    'host': '10.10.20.36',
    'port': 30808,
    'user': 'admin'
}

# Configuration des catalogues Trino
TRINO_CATALOGS = {
    'production': {
        'catalog': 'sime-production',
        'schema': 'dbo'
    },
    'distribution': {
        'catalog': 'sime-distribution',
        'schema': 'dbo'
    },
    'postgresql': {
        'catalog': 'sime-postgresql',
        'schema': 'dbo'
    },
    'minio_test': {
        'catalog': 'minio-test',
        'schema': 'sime-dwh'
    }
}

# Configuration PostgreSQL
POSTGRES_CONFIG = {
    'user': 'postgres',
    'password': 'postgres',
    'host': '10.10.20.36',
    'port': 30894,
    'database': 'postgres_cie'
}

# Configuration Nessie/Trino pour push_df_to_nessie
NESSIE_CONFIG = {
    'trino_user': 'admin',
    'trino_host': '10.10.20.36',
    'trino_port': 30808,
    'trino_catalog': 'minio-test',
    'trino_schema': 'sime-dwh'
}


# Création des connexions Trino (objets manquants)
def create_trino_connection(catalog, schema='dbo'):
    """Crée une connexion Trino pour un catalogue donné"""
    return connect(
        host=TRINO_CONFIG['host'],
        port=TRINO_CONFIG['port'],
        user=TRINO_CONFIG['user'],
        catalog=catalog,
        schema=schema
    )

# Connexions Trino utilisées dans main.py
trino_conn_pro = create_trino_connection('sime-production', 'dbo')
trino_conn_dist = create_trino_connection('sime-distribution', 'dbo') 
trino_conn_post = create_trino_connection('sime-postgresql', 'public')

# Fichiers de données (chemins relatifs depuis le répertoire du script)
import os

def get_data_file_path(filename):
    """Retourne le chemin complet vers un fichier de données"""
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(repo_dir, filename)


# Fichiers de données
DATA_FILES = {
    'objectif_tmc': 'Objectif_tmc_2020_2024.xlsx',
    'structures': 'Structures.xlsx'
}
