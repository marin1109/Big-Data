from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, datediff

spark = SparkSession.builder.appName("Football_Data_Analysis").getOrCreate()

chemin_datasets = "datasets/"

csv_files = [
    "appearances.csv",
    "club_games.csv",
    "clubs.csv",
    "competitions.csv",
    "game_events.csv",
    "game_lineups.csv",
    "games.csv",
    "players.csv",
    "player_valuations.csv",
    "transfers.csv"
]

dataframes = {}

for file in csv_files:
    df_name = file.split(".")[0]
    path = f"{chemin_datasets}{file}"
    df = spark.read.csv(path, header=True, inferSchema=True)
    dataframes[df_name] = df
    print(f"✅ Chargé : {df_name}")
    df.show(5)

for name, df in dataframes.items():
    df.createOrReplaceTempView(name)

print("\n📌 Tables disponibles dans SparkSQL :")
spark.sql("SHOW TABLES").show()

print("\n🔎 Top 10 joueurs enregistrés :")
spark.sql("SELECT * FROM players LIMIT 10").show()

print("\n📊 Nombre total de joueurs :")
spark.sql("SELECT COUNT(*) AS total_joueurs FROM players").show()

print("\n⚽ Clubs avec le plus de joueurs :")
spark.sql("""
    SELECT club_name, COUNT(*) AS nombre_joueurs
    FROM clubs
    GROUP BY club_name
    ORDER BY nombre_joueurs DESC
    LIMIT 10
""").show()

print("\n💰 Dernières transactions de joueurs :")
spark.sql("""
    SELECT * FROM transfers
    ORDER BY transfer_date DESC
    LIMIT 5
""").show()

print("\n💾 Sauvegarde des joueurs âgés de plus de 30 ans dans un fichier CSV...")

players_with_age = dataframes["players"].withColumn("age", (datediff(current_date(), col("date_of_birth")) / 365.25).cast("int"))

players_with_age.createOrReplaceTempView("players")

joueurs_30_plus = spark.sql("SELECT * FROM players WHERE age > 30")

chemin_sauvegarde = "/home/marin/M1/Big-Data/datasets/joueurs_30_plus.csv"
joueurs_30_plus.write.csv(chemin_sauvegarde, header=True)

print(f"\n✅ Fichier sauvegardé : {chemin_sauvegarde}")

print("\n✅ Script terminé avec succès ! 🚀")
