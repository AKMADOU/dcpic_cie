from trino.dbapi import connect

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

# Fichiers de données (chemins relatifs depuis le répertoire du script)
import os

def get_data_file_path(filename):
    """Retourne le chemin complet vers un fichier de données"""
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(repo_dir, filename)

DATA_FILES = {
    'objectif_tmc': 'Objectif_tmc_2020_2024.xlsx',
    'structures': 'Structures.xlsx'
}
