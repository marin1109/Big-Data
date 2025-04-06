from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import time
from time import sleep

# =============================================================================
# Configuration de la SparkSession pour YARN
# =============================================================================
spark = SparkSession.builder \
    .appName("YarnTests") \
    .master("yarn") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# =============================================================================
# Chemin HDFS - veillez à ce que ces fichiers soient présents dans HDFS
# =============================================================================
csv_path = "hdfs://localhost:9000/user/marin/datasets/"

# =============================================================================
# Chargement des DataFrames
# =============================================================================
appearances_df = spark.read.option("header", "true") \
                           .option("inferSchema", "true") \
                           .csv(f"{csv_path}appearances.csv")

player_valuations_df = spark.read.option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv(f"{csv_path}player_valuations.csv")

games_df = spark.read.option("header", "true") \
                     .option("inferSchema", "true") \
                     .csv(f"{csv_path}games.csv")

# =============================================================================
# Partitionnement identique (8 partitions)
# =============================================================================
appearances_df = appearances_df.repartition(8, "player_id").cache()
player_valuations_df = player_valuations_df.repartition(8, "player_id").cache()
games_df = games_df.repartition(8, "date").cache()

# =============================================================================
# Requête 1 : Agrégation
# =============================================================================
print("=== Début Requête 1 (YARN) ===")
start_time = time.time()

result1 = (appearances_df
           .groupBy("player_id")
           .agg(spark_sum("goals").alias("total_goals"))
           .orderBy(col("total_goals").desc())
           .limit(10))

result1.show()
print("Temps d'exécution Requête 1 (YARN) :",
      time.time() - start_time, "secondes")
print("=== Fin Requête 1 ===\n")
sleep(5)

# =============================================================================
# Requête 2 : Jointure
# =============================================================================
print("=== Début Requête 2 (YARN) ===")
start_time = time.time()

result2 = (
    appearances_df.join(
        player_valuations_df,
        on=[appearances_df.player_id == player_valuations_df.player_id,
            appearances_df.date == player_valuations_df.date],
        how="inner"
    )
    .select(
        appearances_df.player_id,
        appearances_df.game_id,
        player_valuations_df.market_value_in_eur
    )
)

total_joined = result2.count()
print("Nombre total de lignes jointes =", total_joined)
result2.show(10)

print("Temps d'exécution Requête 2 (YARN) :",
      time.time() - start_time, "secondes")
print("=== Fin Requête 2 ===\n")
sleep(5)

# =============================================================================
# Requête 3 : Filtrage
# =============================================================================
print("=== Début Requête 3 (YARN) ===")
start_time = time.time()

result3 = games_df.filter(
    (col("date") > "2020-01-01") &
    ((col("home_club_goals") + col("away_club_goals")) >= 3)
)
count_filtered = result3.count()
print("Nombre de matchs filtrés =", count_filtered)
result3.show(10)

print("Temps d'exécution Requête 3 (YARN) :",
      time.time() - start_time, "secondes")
print("=== Fin Requête 3 ===\n")
sleep(5)

# =============================================================================
# Affichage du nombre de partitions
# =============================================================================
print("Partitions appearances_df :", appearances_df.rdd.getNumPartitions())
print("Partitions player_valuations_df :", player_valuations_df.rdd.getNumPartitions())
print("Partitions games_df :", games_df.rdd.getNumPartitions())

spark.stop()
