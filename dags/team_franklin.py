from __future__ import annotations
from  include.team_franklin_spark import run_daily 
import os
import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.sensors.filesystem import FileSensor

# Importations des modules fournis par le kit d'origine
from include.ingest import ingest_day, validate_silver
from include.paths import report_json

# 1. IMPORTATION DE TON SCRIPT PYSPARK INDIVIDUEL (Étape 2 & 4)
# Une fois ton fichier créé dans include/team_franklin_spark.py, décommente la ligne suivante :
# from include.team_franklin_spark import run_daily

DEFAULT_ARGS = {
    "owner": "team_franklin",
    "retries": 0,                      # Track R: Gestion de la fiabilité (essais automatiques)
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="team_franklin",               # Étape 1 : Identifiant unique de ton DAG
    description="Pipeline Capstone de calcul des KPIs de vente au détail",
    start_date=datetime(2026, 6, 1),   # Plage temporelle du projet
    end_date=datetime(2026, 6, 14),     
    schedule="@daily",                 # Exécution logique quotidienne
    catchup=False,                     # Désactivé par défaut, activable pour le backfill
    default_args=DEFAULT_ARGS,
    tags=["lab4", "capstone", "franklin"],
) as dag:

    # -------------------------------------------------------------------------
    # TÂCHE 1 : FileSensor (Vérification de l'arrivée du fichier CSV du fournisseur)
    # -------------------------------------------------------------------------
    wait_csv = FileSensor(
        task_id="wait_for_vendor_csv",
        filepath="incoming/transactions_{{ ds }}.csv", # Interpolation Jinja de la date logique (ds)
        fs_conn_id="fs_default",
        poke_interval=30,               # Intervalle de vérification (secondes)
        timeout=600,                    # Évite de bloquer un worker indéfiniment
        mode="poke",
    )

    # -------------------------------------------------------------------------
    # TÂCHE 2 : Ingestion Bronze -> Silver via DuckDB
    # -------------------------------------------------------------------------
    @task(task_id="ingest_bronze_to_silver")
    def task_ingest(ds=None):
        """Prend le CSV brut et génère un fichier Parquet nettoyé et typé (Silver)"""
        print(f"Début de l'ingestion DuckDB pour la date logique : {ds}")
        ingest_day(ds)                  

    # -------------------------------------------------------------------------
    # TÂCHE 3 : Validation des données Silver (Créativité / Data Quality)
    # -------------------------------------------------------------------------
    @task(task_id="validate_silver_data")
    def task_validate(ds: str):
        """

        Vérifie la conformité des données Silver.

        Si les données sont corrompues, la tâche passe en FAILED dans Airflow.

        """
        print(f"Validation de la qualité des données Silver pour : {ds}")

        is_valid = validate_silver(ds)

        if not is_valid:

            raise AirflowFailException(

                f"Échec de qualité des données pour {ds}. "

                "Les données Silver sont invalides ou corrompues."

            )

        print(f"Validation réussie pour {ds}")
        return True

    # -------------------------------------------------------------------------
    # TÂCHE 4 : Traitement Analytique Gold via PySpark (Ton code Spark central)
    # -------------------------------------------------------------------------
    @task(task_id="compute_pyspark_kpis")
    def task_spark(ds=None):
        """
        Exécute la session de calcul PySpark locale (local[*]).
        Génère le Parquet Gold et le fichier JSON de reporting.
        """
        print(f"Lancement du job d'agrégation PySpark pour : {ds}")
        run_daily(ds)
        

    # -------------------------------------------------------------------------
    # TÂCHE 5 : Diffusion / Sauvegarde secondaire (Créativité / Idempotence)
    # -------------------------------------------------------------------------
    @task(task_id="export_summary_backup")
    def task_export_backup(ds=None):
        """
        Garantit l'idempotence en fin de chaîne et copie le livrable JSON
        généré dans un dossier de sauvegarde secondaire (Serve layer).
        """
        source_json = report_json(ds)
        backup_dir = "data/reports_backup"
        backup_json = f"{backup_dir}/dashboard_{ds}.json"
        
        # Idempotence : Nettoyage d'une ancienne sauvegarde si elle existe déjà pour éviter les doublons
        if os.path.exists(backup_json):
            print(f"Ancien rapport trouvé pour {ds}. Suppression pour idempotence...")
            os.remove(backup_json)
            
        os.makedirs(backup_dir, exist_ok=True)
        
        if os.path.exists(source_json):
            shutil.copy(source_json, backup_json)
            print(f"✅ Rapport exporté avec succès vers la sauvegarde : {backup_json}")
        else:
            raise FileNotFoundError(f"Le fichier source généré est introuvable : {source_json}")

    # -------------------------------------------------------------------------
    # ORCHESTRATION ET CHAINAGE DES DEPENDANCES (Le graphe de flux)
    # -------------------------------------------------------------------------
    # Le pipeline s'exécute linéairement : Attente -> Ingestion -> Validation -> Spark -> Sauvegarde
    wait_csv >> task_ingest() >> task_validate() >> task_spark() >> task_export_backup()