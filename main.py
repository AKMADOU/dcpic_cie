# main.py
"""
Script principal pour le traitement des données
"""

import time
import pandas as pd
from pathlib import Path
from isoweek import Week
import calendar
import math
from trino.dbapi import connect
import os

# Imports des modules personnalisés
from config import TRINO_CONFIG, TRINO_CATALOGS, DATA_FILES, get_data_file_path
from nessie_utils import push_df_to_nessie


def create_trino_connections():
    """Créer les connexions Trino"""
    connections = {}
    
    for name, catalog_config in TRINO_CATALOGS.items():
        connections[name] = connect(
            host=TRINO_CONFIG['host'],
            port=TRINO_CONFIG['port'],
            user=TRINO_CONFIG['user'],
            catalog=catalog_config['catalog'],
            schema=catalog_config['schema']
        )
    
    return connections


def extract_data(connections):
    """Extraire les données depuis Trino"""
    
    # Extraction des données TCEL HTA
    q_tcel_hta = pd.read_sql_query("""
        SELECT 
            libelle_direction AS dr,
            libelle_ouvrage AS poste_source,
            libelle_depart AS depart_el,
            CAST(date_releve AS date) AS date,
            energie_livree AS energie_livree
        FROM "sime-postgresql".public.view_releve_valider_dr_distribution_trino
        WHERE EXTRACT(YEAR FROM date_releve) >= 2025
        ORDER BY date_releve DESC
    """, connections['postgresql'])

    # Extraction des incidents HTB
    q_inci_htb = pd.read_sql_query("""
        SELECT vbe.nom_abrege_d AS DR,
               vbe.poste_nom_site AS poste_source,
               vbe.date_heure_debut,
               vbe.date_heure_fin,
               vbe.nom_expl AS ouvrage,
               vbe.imputation,
               vbe.puissance_coupee,
               date_diff('minute', vbe.date_heure_debut, vbe.date_heure_fin) AS duree_incident,
               vbe.energie_non_dist/1000 AS end_mwh,
               sig.description AS signalisation,
               vbe.lieu_defaut_hta,
               vbe.reen_mode,
               RTRIM(vbe.resp_incident) AS respo,
               ori.description AS origine,
               cause.description AS cause
        FROM "sime-distribution".dbo.view_bcc_exploit vbe
        INNER JOIN "sime-distribution".dbo.bcc_codes_signalisationS_ref sig
            ON sig.code_signal = vbe.signalisation
        INNER JOIN "sime-distribution".dbo.bcc_causes_incidents_hta_ref cause
            ON cause.code_cause = vbe.cause
        INNER JOIN "sime-distribution".dbo.bcc_origine_incidents_ref ori
            ON ori.code_origine_bcc = vbe.code_origine_bcc
        WHERE EXTRACT(YEAR FROM vbe.date_heure_debut) >= 2020
        ORDER BY vbe.date_heure_debut
    """, connections['distribution'])

    # Extraction des manœuvres HTB/HTA
    q_man_htb_hta = pd.read_sql_query("""
        SELECT vmt.date_heure_debut,
               vmt.date_heure_fin,
               vmt.nom_abrege_d,
               vmt.poste_nom_site,
               vmt.nom_expl,
               vmt.lieu_defaut_hta,
               vmt.puissance_coupee,
               vmt."duree_manoeuvres_travaux(mn)" AS duree_manoeuvres_travaux_mn,
               vmt.energie_non_dist,
               vmt.nature,
               vmt.imputation,
               vmt.resp_end,
               vmt.niveau_tension
        FROM "sime-distribution".dbo.bcc_vues_manoeuvres_travaux vmt
        WHERE EXTRACT(YEAR FROM vmt.date_heure_debut) >= 2020
    """, connections['distribution'])

    # Extraction énergies livrées HTA
    q_el_hta = pd.read_sql_query("""
        SELECT elm.*,
               dep.nom_expl AS depart,
               pos.poste_nom_site AS poste_source
        FROM "sime-production".dbo.energies_livrees_mens elm
        INNER JOIN "sime-production".dbo.departs_ref dep
            ON dep.depart_id = elm.depart_id
        INNER JOIN "sime-production".dbo.postes_ref pos
            ON pos.poste_id = dep.poste_id
        WHERE EXTRACT(YEAR FROM elm.date_mois) >= 2018
        ORDER BY elm.date_mois
    """, connections['production'])

    return q_tcel_hta, q_inci_htb, q_man_htb_hta, q_el_hta


def process_energie_data(q_el_hta, q_tcel_hta):
    """Traitement des données d'énergie"""
    
    # Traitement des données SIME
    df_el_hta = pd.DataFrame(q_el_hta)
    df_el_hta = df_el_hta.drop_duplicates(subset=['date_mois', 'depart_id'], keep='last')
    df_el_hta['date_mois'] = pd.to_datetime(df_el_hta['date_mois'])
    df_el_hta_clean = df_el_hta[~df_el_hta.depart.str.contains('ARV|DPT|TFO|D2|D1|D 1|D 2')]

    # Traitement des données TCEL
    df_el_tcel = pd.DataFrame(q_tcel_hta)
    df_el_tcel = df_el_tcel[["date", "energie_livree", "depart_el", "poste_source"]]
    df_el_tcel['date'] = pd.to_datetime(df_el_tcel['date'], errors='coerce')
    df_el_tcel['annee'] = df_el_tcel['date'].dt.year
    df_el_tcel['date_mois'] = df_el_tcel['date'].dt.to_period('M').dt.start_time
    
    # Renommage des colonnes
    column_renoms = {'energie_livree': 'energie', 'depart_el': 'depart'}
    df_el_tcel.rename(columns=column_renoms, inplace=True)
    df_el_tcel = df_el_tcel[["date_mois", "energie", "depart", "poste_source", "annee"]]
    df_el_tcel = df_el_tcel.sort_values(by="date_mois", ascending=True)

    # Traitement SIME final
    df_el_sime = df_el_hta_clean.copy()
    df_el_sime = df_el_sime.drop_duplicates()
    df_el_sime['annee'] = df_el_sime['date_mois'].dt.year
    df_el_sime = df_el_sime[df_el_sime['annee'] >= 2020]
    df_el_sime = df_el_sime[['date_mois', 'energie', 'depart', 'poste_source', 'annee']]

    # Concaténation
    df_el = pd.concat([df_el_sime, df_el_tcel], ignore_index=True)
    
    # Ajout des dimensions temporelles
    df_el['debut mois'] = df_el['date_mois'].dt.to_period('M').dt.start_time
    df_el['numsem_iso'] = df_el['date_mois'].apply(lambda x: Week.withdate(x).week)
    df_el['annee_iso'] = df_el['date_mois'].dt.year
    df_el['mois_mmm'] = df_el['date_mois'].dt.strftime('%b')
    df_el['mois_mmm_aa'] = df_el['date_mois'].dt.strftime('%b-%y')
    df_el['rg_semaine'] = (df_el['date_mois'] - df_el['date_mois'].min()).dt.days // 7 + 1
    df_el['numsem_iso'] = df_el['numsem_iso'].astype(str).str.zfill(2)
    df_el['num_sem'] = df_el['annee_iso'].astype(str) + ' - S ' + df_el['numsem_iso']
    df_el['num_mois'] = df_el['date_mois'].dt.month
    df_el['mois'] = df_el['date_mois'].dt.strftime('%Y-%m')

    return df_el, df_el_hta_clean, df_el_tcel


def nombre_heures_total(mois, annee, energie_livree):
    """Calculer le nombre d'heures total dans un mois"""
    if pd.notna(energie_livree):
        jours_dans_mois = calendar.monthrange(annee, list(calendar.month_abbr).index(mois))[1]
        return jours_dans_mois * 24
    else:
        return 0


def process_power_calculations(df_el):
    """Traitement des calculs de puissance"""
    
    df_el['nombre_heures'] = df_el.apply(
        lambda row: nombre_heures_total(row['mois_mmm'], row['annee'], row['energie']), 
        axis=1
    )
    
    dico = df_el.groupby(['mois_mmm_aa'])['energie'].sum().to_dict()
    df_el['energie_mois'] = df_el.mois_mmm_aa.map(dico)
    df_el['PmoyM'] = df_el['energie_mois'] / df_el['nombre_heures']

    dico2 = df_el.groupby(['annee', 'num_mois'])['energie'].sum().to_dict()

    annee = []
    mois = []
    energie = []

    for key, value in dico2.items():
        annee.append(key[0])
        mois.append(key[1])
        energie.append(value)

    df = pd.DataFrame({
        'annee': annee,
        'mois': mois,
        'energie': energie
    })

    df = df.sort_values(['annee', 'mois'])
    df = df.drop(df.index[-1])
    df = df.sort_values(['annee', 'mois'])

    df_el_merge = df_el.rename(columns={'num_mois': 'mois'})[['annee', 'mois', 'nombre_heures']]
    df_el_merge = df_el_merge.loc[:, ~df_el_merge.columns.duplicated()]
    df = df.merge(df_el_merge, on=['annee', 'mois'], how='left').drop_duplicates()

    df['energie_Cumul'] = df.groupby('annee')['energie'].cumsum()
    df['NBH_Cumul'] = df.groupby('annee')['nombre_heures'].cumsum()

    df['PmoyJ'] = df['energie'] / (31 * 24)
    df['PmoyH'] = (df['energie'] / 4) / 168
    df['PmoyM'] = df['energie'] / df['nombre_heures']
    df['PmoyA'] = df['energie_Cumul'] / df['NBH_Cumul']

    df['PmoyJ'] = df['PmoyJ'] / 1000
    df['PmoyM'] = df['PmoyM'] / 1000
    df['PmoyH'] = df['PmoyH'] / 1000
    df['PmoyA'] = df['PmoyA'] / 1000

    # Ajout de nouvelles lignes
    derniere_ligne = df.iloc[-1]
    nouveau_mois = derniere_ligne["mois"] + 1
    nouvelle_année = derniere_ligne["annee"]
    
    if nouveau_mois > 12:
        nouveau_mois = 1
        nouvelle_année += 1
        nouvelle_energie_cumul = derniere_ligne["energie"]
    else:
        nouvelle_energie_cumul = derniere_ligne["energie_Cumul"] + derniere_ligne["energie"]

    nouvelle_ligne = {
        "annee": nouvelle_année,
        "mois": nouveau_mois,
        "energie": derniere_ligne["energie"],
        "nombre_heures": derniere_ligne["nombre_heures"],
        "energie_Cumul": nouvelle_energie_cumul,
        "NBH_Cumul": derniere_ligne["NBH_Cumul"],
        "PmoyJ": derniere_ligne["PmoyJ"],
        "PmoyH": derniere_ligne["PmoyH"],
        "PmoyM": derniere_ligne["PmoyM"],
        "PmoyA": derniere_ligne["PmoyA"],
    }

    df = pd.concat([df, pd.DataFrame([nouvelle_ligne])], ignore_index=True)

    new_row = pd.DataFrame({
        'annee': [2025], 
        'mois': [7], 
        'energie': [1.189239e+09], 
        'nombre_heures': [744.0], 
        'energie_Cumul': [7.130640e+09 + 1.189239e+09], 
        'NBH_Cumul': [3624.0], 
        'PmoyJ': [1598.440039], 
        'PmoyH': [1769.701472],
        'PmoyM': [1598.440039], 
        'PmoyA': [1639.459228]
    })
    df = pd.concat([df, new_row], ignore_index=True)

    return df


def process_incidents_data(q_inci_htb):
    """Traitement des données d'incidents"""
    
    df_inci_htb = pd.DataFrame(q_inci_htb)
    df_inci_htb['date_heure_debut'] = pd.to_datetime(df_inci_htb['date_heure_debut'])
    
    df_inci = df_inci_htb.copy()
    df_inci = df_inci.drop_duplicates()
    df_inci.dropna(subset=['imputation'], inplace=True)

    df_inci['date_heure_debut'] = pd.to_datetime(df_inci['date_heure_debut'])
    df_inci['date_heure_fin'] = pd.to_datetime(df_inci['date_heure_fin'])
    df_inci['duree_minutes'] = (df_inci['date_heure_fin'] - df_inci['date_heure_debut']).dt.total_seconds() / 60
    df_inci['duree_heures'] = (df_inci['date_heure_fin'] - df_inci['date_heure_debut']).dt.total_seconds() / 3600
    df_inci.loc[df_inci['date_heure_debut'].isnull() | df_inci['date_heure_fin'].isnull(), ['duree_minutes', 'duree_heures']] = 0
    df_inci = df_inci.drop(columns=['end_mwh'])
    df_inci['end_mwh'] = df_inci['puissance_coupee'] * df_inci['duree_heures']
    
    def calcul_end2(row):
        if row['duree_minutes'] <= 3:
            return 0
        else:
            return row['end_mwh']

    df_inci['end_mwh_2'] = df_inci.apply(calcul_end2, axis=1)
    df_inci_filtre = df_inci[['date_heure_debut', 'duree_incident', 'imputation', 'poste_source', 'ouvrage', 'end_mwh', 'end_mwh_2', 'respo', 'puissance_coupee', 'cause', 'signalisation']]

    column_renom = {'duree_incident': 'duree', 'poste_source': 'poste', 'end_mwh': 'end (mwh)', 'cause': 'cause incident'}
    df_inci_filtre.rename(columns=column_renom, inplace=True)
    df_inci_filtre['date'] = df_inci_filtre['date_heure_debut'].dt.date
    df_inci_filtre['heure'] = df_inci_filtre['date_heure_debut'].dt.time
    df_inci_filtre['typologie'] = 'incident'
    df_inci_filtre['imputation'] = df_inci_filtre['imputation'].str.strip()

    return df_inci_filtre, df_inci_htb


def process_maneuvers_data(q_man_htb_hta):
    """Traitement des données de manœuvres"""
    
    df_man_htb = pd.DataFrame(q_man_htb_hta)
    df_man_htb['date_heure_debut'] = pd.to_datetime(df_man_htb['date_heure_debut'])
    
    df_man = df_man_htb.copy()
    df_man = df_man.drop_duplicates()
    df_man.dropna(subset=['imputation'], inplace=True)
    df_man.dropna(subset=['resp_end'], inplace=True)
    df_man['duree_minutes'] = (df_man['date_heure_fin'] - df_man['date_heure_debut']).dt.total_seconds() / 60
    df_man['duree_heures'] = (df_man['date_heure_fin'] - df_man['date_heure_debut']).dt.total_seconds() / 3600
    df_man.loc[df_man['date_heure_debut'].isnull() | df_man['date_heure_fin'].isnull(), ['duree_minutes', 'duree_heures']] = 0
    df_man = df_man.drop(columns=['energie_non_dist'])
    df_man['energie_non_dist'] = df_man['puissance_coupee'] * df_man['duree_heures']
    
    def calcul_end_man(row):
        if row['duree_minutes'] <= 3:
            return 0
        else:
            return row['energie_non_dist']

    df_man['end_mwh_2'] = df_man.apply(calcul_end_man, axis=1)
    df_man_filtre = df_man[['date_heure_debut', 'duree_minutes', 'imputation', 'energie_non_dist', 'end_mwh_2', 'puissance_coupee', 'nature', 'nom_expl', 'poste_nom_site']]
    
    column_renoms = {'duree_minutes': 'duree', 'poste_nom_site': 'poste', 'nom_expl': 'ouvrage', 'energie_non_dist': 'end (mwh)', 'nature': 'nature manoeuvre'}
    df_man_filtre.rename(columns=column_renoms, inplace=True)
    df_man_filtre['imputation'] = df_man_filtre['imputation'].str.strip()
    df_man_filtre['date'] = df_man_filtre['date_heure_debut'].dt.date
    df_man_filtre['heure'] = df_man_filtre['date_heure_debut'].dt.time
    df_man_filtre['typologie'] = 'Manoeuvre'

    return df_man_filtre, df_man_htb


def process_combined_data(df_inci_filtre, df_man_filtre):
    """Combinaison et traitement des données incidents/manœuvres"""
    
    df_inci_man = pd.concat([df_inci_filtre, df_man_filtre], ignore_index=True)
    df_inci_man = df_inci_man.sort_values(by='date', ascending=True)
    df_inci_man['nature manoeuvre'] = df_inci_man['nature manoeuvre'].str.strip()
    df_inci_man['date'] = pd.to_datetime(df_inci_man['date'], format='%Y-%m-%d', errors='coerce')
    df_inci_man['heure'] = pd.to_datetime(df_inci_man['heure'], format='%H:%M:%S', errors='coerce')
    df_inci_man['annee'] = df_inci_man['date'].dt.year
    df_inci_man = df_inci_man[df_inci_man['annee'] >= 2020]

    # Ajout des dimensions temporelles
    df_inci_man['debut semaine'] = df_inci_man['date'].dt.to_period('W').dt.start_time
    df_inci_man['debut mois'] = df_inci_man['date'].dt.to_period('M').dt.start_time
    df_inci_man['annee-mois'] = df_inci_man['date'].dt.strftime('%Y%m')
    df_inci_man['mois'] = df_inci_man['date'].dt.strftime('%Y-%m')
    df_inci_man['mois_mmm'] = df_inci_man['date'].dt.strftime('%b')
    df_inci_man['num_mois'] = df_inci_man['date'].dt.month
    df_inci_man['mois_mmm_aa'] = df_inci_man['date'].dt.strftime('%b-%y')

    df_inci_man['numsem_iso'] = df_inci_man['date'].apply(lambda x: Week.withdate(x).week)
    df_inci_man['annee_iso'] = df_inci_man['date'].dt.year

    df_inci_man['rg_semaine'] = (df_inci_man['debut semaine'] - df_inci_man['debut semaine'].min()).dt.days // 7 + 1
    df_inci_man['semaine'] = 'Sem ' + df_inci_man['numsem_iso'].astype(str).str.zfill(2)
    df_inci_man['yearstart'] = df_inci_man['date'].dt.to_period('Y').dt.start_time
    df_inci_man['yearstartday'] = df_inci_man['yearstart'].dt.weekday + 1
    df_inci_man['numsem_iso'] = df_inci_man['numsem_iso'].astype(str).str.zfill(2)
    df_inci_man['semaine_full'] = df_inci_man['annee_iso'].astype(str) + ' - S ' + df_inci_man['numsem_iso']

    return df_inci_man


def load_objectives_data():
    """Charger les données d'objectifs TMC"""
    excel_path = get_data_file_path(DATA_FILES['objectif_tmc'])
    obj_tmc = pd.read_excel(excel_path)
    obj_tmc['annee'] = obj_tmc['DEBUT MOIS'].dt.year
    obj_tmc['cumul obj tmc'] = obj_tmc.groupby('annee')['OBJECTIF TMC'].cumsum()
    obj_tmc = obj_tmc[['DEBUT MOIS', 'OBJECTIF TMC', 'cumul obj tmc']]
    obj_tmc.columns = obj_tmc.columns.str.lower()
    return obj_tmc


def merge_with_objectives(df_inci_man, obj_tmc):
    """Fusionner avec les objectifs TMC"""
    inci_man_obj = pd.merge(df_inci_man, obj_tmc, on='debut mois')
    inci_man_obj['obj tmc'] = inci_man_obj['cumul obj tmc'].round(0)
    inci_man_obj = inci_man_obj.drop('objectif tmc', axis=1)
    inci_man_obj = inci_man_obj.sort_values(by='date')
    return inci_man_obj


def merge_with_energy_data(inci_man_obj, df_power):
    """Fusionner avec les données d'énergie"""
    join_df = pd.merge(
        inci_man_obj, 
        df_power[['annee', 'mois', 'energie', 'energie_Cumul', 'PmoyJ', 'PmoyH', 'PmoyM', 'PmoyA']],
        left_on=['annee', 'num_mois'],
        right_on=['annee', 'mois'],
        how='left'
    )

    join_df['energie Liv (GWh)'] = join_df['energie'] / 1000000
    join_df['energie cum Liv (GWh)'] = join_df['energie_Cumul'] / 1000000

    # Calcul des TMC
    join_df['end (mwh)'] = join_df['end (mwh)'] / 1000
    join_df['end_mwh_2'] = join_df['end_mwh_2'] / 1000

    join_df['TMC hh (PmoyH)'] = join_df['end_mwh_2'] / join_df['PmoyH']
    join_df['TMC hh (PmoyJ)'] = join_df['end_mwh_2'] / join_df['PmoyJ']
    join_df['TMC jj (PmoyH)'] = join_df['TMC hh (PmoyH)'] / 24
    join_df['TMC mm (PmoyH)'] = join_df['TMC hh (PmoyH)'] * 60
    join_df['TMC mm (PmoyJ)'] = join_df['TMC hh (PmoyJ)'] * 60

    return join_df


def load_structures_data():
    """Charger les données de structures"""
    excel_path = get_data_file_path(DATA_FILES['structures'])
    struct = pd.read_excel(excel_path)
    struct['IMPUTATION'] = struct['IMPUTATION'].str.strip()
    struct['GROUPEMENT'] = struct['GROUPEMENT'].str.strip()
    struct['SEGMENT'] = struct['SEGMENT'].str.strip()
    struct = struct.drop_duplicates()
    struct.columns = struct.columns.str.lower()
    return struct


def merge_with_structures(join_df, struct):
    """Fusionner avec les données de structures"""
    join_df['imputation'] = join_df['imputation'].str.strip()
    join_data = pd.merge(join_df, struct, on=['imputation'], how='left')
    join_data = join_data.drop(['mois_y'], axis=1)

    column_renoms3 = {
        'mois_x': 'mois',
        'mois_mmm': 'mois MMM',
        'num_mois': 'NUM mois',
        'mois_mmm_aa': 'mois MMM-AA',
        'numsem_iso': 'NUMSEM ISO',
        'annee_iso': 'annee ISO',
        'rg_semaine': 'RG semaine',
        'semaine_full': 'semaine FULL'
    }
    join_data.rename(columns=column_renoms3, inplace=True)

    # Nettoyage des données nulles
    join_data.dropna(subset=['segment'], inplace=True)

    return join_data


def main():
    """Fonction principale"""
    print("🚀 Début du traitement des données...")
    
    # 1. Création des connexions Trino
    print("📡 Création des connexions Trino...")
    connections = create_trino_connections()
    
    # 2. Extraction des données
    print("📊 Extraction des données...")
    q_tcel_hta, q_inci_htb, q_man_htb_hta, q_el_hta = extract_data(connections)
    
    # 3. Traitement des données d'énergie
    print("⚡ Traitement des données d'énergie...")
    df_el, df_el_hta_clean, df_el_tcel = process_energie_data(q_el_hta, q_tcel_hta)
    df_power = process_power_calculations(df_el)
    
    # 4. Traitement des incidents et manœuvres
    print("🔧 Traitement des incidents et manœuvres...")
    df_inci_filtre, df_inci_htb = process_incidents_data(q_inci_htb)
    df_man_filtre, df_man_htb = process_maneuvers_data(q_man_htb_hta)
    df_inci_man = process_combined_data(df_inci_filtre, df_man_filtre)
    
    # 5. Chargement et fusion des données d'objectifs
    print("🎯 Chargement des objectifs TMC...")
    obj_tmc = load_objectives_data()
    inci_man_obj = merge_with_objectives(df_inci_man, obj_tmc)
    
    # 6. Fusion avec les données d'énergie
    print("🔗 Fusion avec les données d'énergie...")
    join_df = merge_with_energy_data(inci_man_obj, df_power)
    
    # 7. Chargement et fusion des structures
    print("🏗️ Chargement des structures...")
    struct = load_structures_data()
    join_data = merge_with_structures(join_df, struct)
    
    # 8. Envoi vers Nessie
    print("📤 Envoi des données vers Nessie...")
    push_df_to_nessie(df=join_data, pg_table_name="temp_df_data", trino_table_name="data")
    push_df_to_nessie(df=df_man_htb, pg_table_name="temp_df_db_man_htb_hta_2", trino_table_name="db_man_htb_hta_2")
    push_df_to_nessie(df=df_inci_htb, pg_table_name="temp_df_db_inci_htb", trino_table_name="db_inci_htb")
    push_df_to_nessie(df=df_el_tcel, pg_table_name="temp_df_db_tcel_hta", trino_table_name="db_tcel_hta")
    push_df_to_nessie(df=df_el_hta_clean, pg_table_name="temp_df_db_el_hta", trino_table_name="db_el_hta")
    
    print("✅ Traitement terminé avec succès!")
    
    # Fermeture des connexions
    for conn in connections.values():
        conn.close()


if __name__ == "__main__":
    main()
