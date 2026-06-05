from __future__ import annotations

import os
import json
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# -------------------------------------------------------------------------
# DÉFINITION DU SCHÉMA STRICT (Sécurité & Track S: Spark Depth)
# -------------------------------------------------------------------------
# Le fournisseur génère des colonnes réalistes (tx_id, category, country, amount_eur...)
SILVER_SCHEMA = StructType([
    StructField("tx_id", StringType(), False),
    StructField("category", StringType(), True),
    StructField("country", StringType(), True),
    StructField("amount_eur", DoubleType(), True),
])

def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:
    """
    Transformation 1 : Lecture de la couche Silver avec schéma strict et filtrage.
    """
    silver_path = f"data/raw/dt={logical_date}/"
    print(f"[Spark] Lecture des données Silver depuis : {silver_path}")
    
    if not os.path.exists(silver_path):
        raise FileNotFoundError(f"Aucune donnée Silver trouvée pour la date {logical_date}")
        
    # Lecture avec application du schéma explicite
    df = spark.read.schema(SILVER_SCHEMA).parquet(silver_path)
    
    # Filtrage de sécurité : Élimination des lignes sans ID ou avec des montants aberrants
    df_filtered = df.filter((F.col("tx_id").isNotNull()) & (F.col("amount_eur") >= 0))
    
    return df_filtered


def transform_2(spark: SparkSession, df: DataFrame, logical_date: str, with_reference: bool = False) -> DataFrame:
    """
    Transformation 2 : Enrichissement des données et dérivation de colonnes.
    """
    # Dérivation de colonnes temporelles basées sur la date logique du jour
    df_enriched = df.withColumn("ingest_day", F.lit(logical_date)) \
                    .withColumn("processed_at", F.current_timestamp())
    
    # Optionnel (Track S/Track Reference) : Jointure avec les cibles de catégories si demandée
    ref_path = "data/reference/category_targets.csv"
    if with_reference and os.path.exists(ref_path):
        print(f"[Spark] Enrichissement via jointure avec le fichier de référence : {ref_path}")
        df_ref = spark.read.option("header", "true").option("inferSchema", "true").csv(ref_path)
        # Utilisation d'un Broadcast Join (Optimisation Spark classique pour les petits volumes)
        df_enriched = df_enriched.join(F.broadcast(df_ref), on="category", how="left")
        
    return df_enriched


def transform_3(df: DataFrame) -> DataFrame:
    """
    Transformation 3 : Agrégation des KPIs demandés par les Opérations.
    Calcul du chiffre d'affaires total et du volume de transactions par catégorie et pays.
    """
    df_aggregated = df.groupBy("category", "country") \
                      .agg(
                          F.round(F.sum("amount_eur"), 2).alias("revenue_eur"),
                          F.count("tx_id").alias("transaction_count")
                      )
    return df_aggregated


def run_daily(logical_date: str, *, with_reference: bool = False) -> dict:
    """
    Point d'entrée principal appelé depuis la tâche Airflow @task.
    Enchaîne transform_1 -> transform_2 -> transform_3 et écrit les résultats.
    """
    # Initialisation de la SparkSession locale (un seul JVM utilisant tous les cœurs du conteneur)
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName(f"TeamFranklin_Spark_{logical_date}") \
        .getOrCreate()
        
    try:
        # --- 1. Chaînage des 3 Transformations ---
        df_silver = transform_1(spark, logical_date)
        df_gold = transform_2(spark, df_silver, logical_date, with_reference=with_reference)
        df_kpis = transform_3(df_gold)
        
        # Pour éviter les multiples recalculs lors des écritures d'outputs (Action Spark)
        df_kpis.cache()
        
        # --- 2. Écriture de la couche GOLD (Idempotence par écrasement 'overwrite') ---
        curated_path = f"data/curated/dt={logical_date}/"
        print(f"[Spark] Écriture des données Gold (Parquet) vers : {curated_path}")
        
        df_kpis.write \
            .mode("overwrite") \
            .parquet(curated_path)
            
        # --- 3. Génération du rapport de la couche SERVE (JSON) ---
        # Collecte des totaux pour construire le tableau de bord léger attendu
        totals = df_kpis.select(
            F.round(F.sum("revenue_eur"), 2).alias("total_revenue"),
            F.sum("transaction_count").alias("total_transactions")
        ).collect()[0]
        
        metrics = {
            "logical_date": logical_date,
            "status": "success",
            "total_revenue_eur": totals["total_revenue"] if totals["total_revenue"] is not None else 0.0,
            "total_transactions_count": totals["total_transactions"] if totals["total_transactions"] is not None else 0,
            "paths": {
                "gold_parquet": curated_path
            }
        }
        
        # Écriture physique du fichier JSON final
        report_path = f"data/reports/dashboard_{logical_date}.json"
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        with open(report_path, "w") as f:
            json.dump(metrics, f, indent=4)
            
        print(f"✅ [Spark] Rapport JSON généré avec succès : {report_path}")
        return metrics

    finally:
        # Fermeture propre de la session pour libérer la mémoire du conteneur
        spark.stop()