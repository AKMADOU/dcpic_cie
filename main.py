# main.py
import pandas as pd
import calendar
from isoweek import Week
from config import trino_conn_pro, trino_conn_dist, trino_conn_post
from nessie_utils import push_df_to_nessie

# ================================
# 1️⃣ Requêtes SQL
# ================================
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
""", trino_conn_post)

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
""", trino_conn_dist)

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
""", trino_conn_dist)

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
""", trino_conn_pro)


# ================================
# 2️⃣ Prétraitement DataFrames
# ================================

# df_el_hta
df_el_hta = pd.DataFrame(q_el_hta).drop_duplicates(subset=['date_mois', 'depart_id'], keep='last')
df_el_hta['date_mois'] = pd.to_datetime(df_el_hta['date_mois'])
df_el_hta_clean = df_el_hta[~df_el_hta.depart.str.contains('ARV|DPT|TFO|D2|D1|D 1|D 2')]

# df_tcel_hta
df_el_tcel = pd.DataFrame(q_tcel_hta)
df_el_tcel['date'] = pd.to_datetime(df_el_tcel['date'], errors='coerce')
df_el_tcel['annee'] = df_el_tcel['date'].dt.year
df_el_tcel['date_mois'] = df_el_tcel['date'].dt.to_period('M').dt.start_time
df_el_tcel.rename(columns={'energie_livree':'energie', 'depart_el':'depart'}, inplace=True)
df_el_tcel = df_el_tcel[['date_mois','energie','depart','poste_source','annee']]
df_el_tcel = df_el_tcel.sort_values('date_mois')

# Fusion énergies
df_el_sime = df_el_hta_clean[['date_mois','energie','depart','poste_source','annee']]
df_el = pd.concat([df_el_sime, df_el_tcel], ignore_index=True)
df_el['debut mois'] = df_el['date_mois'].dt.to_period('M').dt.start_time
df_el['numsem_iso'] = df_el['date_mois'].apply(lambda x: Week.withdate(x).week)
df_el['annee_iso'] = df_el['date_mois'].dt.year
df_el['mois_mmm_aa'] = df_el['date_mois'].dt.strftime('%b-%y')
df_el['nombre_heures'] = df_el.apply(lambda row: calendar.monthrange(row['annee'], row['date_mois'].month)[1]*24, axis=1)
df_el['energie_mois'] = df_el.groupby('mois_mmm_aa')['energie'].transform('sum')
df_el['PmoyM'] = df_el['energie_mois']/df_el['nombre_heures']

# ================================
# Incidents et Manoeuvres
# ================================

def preprocess_inci_man(df_inci, df_man):
    for df in [df_inci, df_man]:
        df.drop_duplicates(inplace=True)
        df.dropna(subset=['imputation'], inplace=True)
        df['date_heure_debut'] = pd.to_datetime(df['date_heure_debut'])
        df['date_heure_fin'] = pd.to_datetime(df['date_heure_fin'])
        df['duree_minutes'] = (df['date_heure_fin'] - df['date_heure_debut']).dt.total_seconds()/60
        df['duree_heures'] = (df['date_heure_fin'] - df['date_heure_debut']).dt.total_seconds()/3600
        df.loc[df['date_heure_debut'].isnull() | df['date_heure_fin'].isnull(), ['duree_minutes','duree_heures']] = 0

    # Energie pour incidents
    df_inci['end_mwh'] = df_inci['puissance_coupee'] * df_inci['duree_heures']
    df_inci['end_mwh_2'] = df_inci.apply(lambda row: 0 if row['duree_minutes']<=3 else row['end_mwh'], axis=1)

    # Energie pour manoeuvres
    df_man['end_mwh_2'] = df_man.apply(lambda row: 0 if row['duree_minutes']<=3 else row['puissance_coupee']*row['duree_heures'], axis=1)

    # Sélection colonnes
    df_inci_filtre = df_inci[['date_heure_debut','duree_incident','imputation','poste_source','ouvrage','end_mwh','end_mwh_2','respo','puissance_coupee','cause','signalisation']].copy()
    df_inci_filtre.rename(columns={'duree_incident':'duree','poste_source':'poste','end_mwh':'end (mwh)','cause':'cause incident'}, inplace=True)
    df_inci_filtre['typologie'] = 'incident'
    df_inci_filtre['date'] = df_inci_filtre['date_heure_debut'].dt.date
    df_inci_filtre['heure'] = df_inci_filtre['date_heure_debut'].dt.time
    df_inci_filtre['imputation'] = df_inci_filtre['imputation'].str.strip()

    df_man_filtre = df_man[['date_heure_debut','duree_minutes','imputation','puissance_coupee','end_mwh_2','nature','nom_expl','poste_nom_site']].copy()
    df_man_filtre.rename(columns={'duree_minutes':'duree','poste_nom_site':'poste','nom_expl':'ouvrage','puissance_coupee':'end (mwh)','nature':'nature manoeuvre'}, inplace=True)
    df_man_filtre['typologie'] = 'Manoeuvre'
    df_man_filtre['date'] = df_man_filtre['date_heure_debut'].dt.date
    df_man_filtre['heure'] = df_man_filtre['date_heure_debut'].dt.time
    df_man_filtre['imputation'] = df_man_filtre['imputation'].str.strip()

    df_inci_man = pd.concat([df_inci_filtre, df_man_filtre], ignore_index=True)
    df_inci_man.sort_values('date', inplace=True)
    return df_inci_man

df_inci_man = preprocess_inci_man(pd.DataFrame(q_inci_htb), pd.DataFrame(q_man_htb_hta))

# ================================
# Lecture objectifs et merge
# ================================
obj_tmc = pd.read_excel("Objectif_tmc_2020_2024.xlsx")
obj_tmc['annee'] = obj_tmc['DEBUT MOIS'].dt.year
obj_tmc['cumul obj tmc'] = obj_tmc.groupby('annee')['OBJECTIF TMC'].cumsum()
obj_tmc = obj_tmc[['DEBUT MOIS','cumul obj tmc']]
obj_tmc.columns = obj_tmc.columns.str.lower()

inci_man_obj = pd.merge(df_inci_man, obj_tmc, left_on='debut mois', right_on='debut mois', how='left')
inci_man_obj['obj tmc'] = inci_man_obj['cumul obj tmc'].round(0)
inci_man_obj.drop('cumul obj tmc', axis=1, inplace=True)

# ================================
# Merge avec énergies
# ================================
df_energie = df_el[['annee','mois','energie','nombre_heures']].drop_duplicates()
join_df = pd.merge(inci_man_obj, df_energie, left_on=['annee','num_mois'], right_on=['annee','mois'], how='left')

join_df['energie Liv (GWh)'] = join_df['energie']/1e6
join_df['energie cum Liv (GWh)'] = join_df['energie'].cumsum()/1e6

# ================================
# Lecture structures et merge
# ================================
struct = pd.read_excel("Structures.xlsx")
struct.columns = struct.columns.str.lower()
struct['imputation'] = struct['imputation'].str.strip()
struct['groupement'] = struct['groupement'].str.strip()
struct['segment'] = struct['segment'].str.strip()
struct.drop_duplicates(inplace=True)

join_data = pd.merge(join_df, struct, on='imputation', how='left')
join_data.dropna(subset=['segment'], inplace=True)

# ================================
# 3️⃣ Push vers Nessie
# ================================
push_df_to_nessie(df=join_data, pg_table_name="temp_df_data", trino_table_name="data")
push_df_to_nessie(df=pd.DataFrame(q_man_htb_hta), pg_table_name="temp_df_db_man_htb_hta_2", trino_table_name="db_man_htb_hta_2")
push_df_to_nessie(df=pd.DataFrame(q_inci_htb), pg_table_name="temp_df_db_inci_htb", trino_table_name="db_inci_htb")
push_df_to_nessie(df=df_el_hta, pg_table_name="temp_df_db_tcel_hta", trino_table_name="db_tcel_hta")
push_df_to_nessie(df=df_el_hta_clean, pg_table_name="temp_df_db_el_hta", trino_table_name="db_el_hta")
