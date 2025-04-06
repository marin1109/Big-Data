from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
from time import sleep

# Initialisation de SparkSession pour YARN
spark = SparkSession.builder \
    .appName("YarnTests") \
    .master("yarn") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

csv_path = "hdfs://localhost:9000/user/marin/datasets/"

# Chargement des données
appearances_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{csv_path}appearances.csv")
player_valuations_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{csv_path}player_valuations.csv")
games_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{csv_path}games.csv")

# Repartitionnement à 4 partitions
appearances_df = appearances_df.repartition(4, "player_id").cache()
player_valuations_df = player_valuations_df.repartition(4, "player_id").cache()
games_df = games_df.repartition(4, "game_id").cache()

# Requête 1 : Top 10 joueurs par buts marqués
print('Début Requête 1')
start_time = time.time()
result1 = appearances_df.groupBy("player_id").sum("goals").orderBy("sum(goals)", ascending=False).limit(10)
result1.show()
print("Temps d'exécution Requête 1 (YARN) : ", time.time() - start_time, " secondes")
print('Fin Requête 1')
sleep(5)

# Requête 2 : Jointure entre appearances et player_valuations
print('Début Requête 2')
start_time = time.time()
result2 = appearances_df.join(player_valuations_df, 
                             (appearances_df.player_id == player_valuations_df.player_id) & 
                             (appearances_df.date == player_valuations_df.date), 
                             "inner").select(appearances_df.player_id, appearances_df.game_id, player_valuations_df.market_value_in_eur)
result2.show(10)
print("Temps d'exécution Requête 2 (YARN) : ", time.time() - start_time, " secondes")
print('Fin Requête 2')
sleep(5)

# Requête 3 : Filtre sur les matchs après 2020 avec plus de 2 buts
print('Début Requête 3')
start_time = time.time()
result3 = games_df.filter((col("date") > "2020-01-01") & (col("home_club_goals") + col("away_club_goals") > 2))
result3.show(10)
print("Temps d'exécution Requête 3 (YARN) : ", time.time() - start_time, " secondes")
print('Fin Requête 3')
sleep(5)

# Affichage du nombre de partitions
print("Partitions appearances_df :", appearances_df.rdd.getNumPartitions())
print("Partitions player_valuations_df :", player_valuations_df.rdd.getNumPartitions())
print("Partitions games_df :", games_df.rdd.getNumPartitions())

# Arrêt de la session Spark
spark.stop()