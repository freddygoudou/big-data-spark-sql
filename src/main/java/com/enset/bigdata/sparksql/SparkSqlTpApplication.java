package com.enset.bigdata.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Objects;

public class SparkSqlTpApplication {

    public static void main(String[] args) {

        // ─────────────────────────────────────────────────────────
        // Initialisation de la session Spark
        // ─────────────────────────────────────────────────────────
        SparkSession spark = SparkSession.builder()
                .appName("TP_Spark_SQL_Bike_Sharing")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Chemin vers le CSV (dans src/main/resources, copié dans le JAR)
        String csvPath = SparkSqlTpApplication.class
                .getClassLoader()
                .getResource("bike_sharing.csv")
                .getPath();

        printSeparator("TP SPARK SQL — Système de Vélos en Libre-Service");

        // ─────────────────────────────────────────────────────────
        // EXERCICE 1 — Chargement & Exploration des données
        // ─────────────────────────────────────────────────────────
        printSection("EXERCICE 1 : Chargement & Exploration");

        // 1.1 Chargement du CSV dans un DataFrame Spark
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .csv(csvPath);

        // 1.2 Affichage du schéma
        System.out.println("── 1.2 Schéma du DataFrame ──");
        df.printSchema();

        // 1.3 Affichage des 5 premières lignes
        System.out.println("── 1.3 Les 5 premières lignes ──");
        df.show(5, false);

        // 1.4 Nombre total de locations
        long totalRentals = df.count();
        System.out.println("── 1.4 Nombre total de locations : " + totalRentals + "\n");


        // ─────────────────────────────────────────────────────────
        // EXERCICE 2 — Création de la vue temporaire SQL
        // ─────────────────────────────────────────────────────────
        printSection("EXERCICE 2 : Création de la vue temporaire");

        df.createOrReplaceTempView("bike_rentals_view");
        System.out.println("✔  Vue temporaire 'bike_rentals_view' créée avec succès.\n");


        // ─────────────────────────────────────────────────────────
        // EXERCICE 3 — Requêtes SQL de base
        // ─────────────────────────────────────────────────────────
        printSection("EXERCICE 3 : Requêtes SQL de base");

        // 3.1 Locations de plus de 30 minutes
        System.out.println("── 3.1 Locations de plus de 30 minutes ──");
        spark.sql("""
                SELECT rental_id, user_id, start_station, end_station, duration_minutes
                FROM bike_rentals_view
                WHERE duration_minutes > 30
                ORDER BY duration_minutes DESC
                """).show(false);

        // 3.2 Locations commençant à "Station A"
        System.out.println("── 3.2 Locations démarrant à Station A ──");
        spark.sql("""
                SELECT rental_id, user_id, start_time, end_station, duration_minutes
                FROM bike_rentals_view
                WHERE start_station = 'Station A'
                ORDER BY start_time
                """).show(false);

        // 3.3 Revenu total
        System.out.println("── 3.3 Revenu total ──");
        spark.sql("""
                SELECT ROUND(SUM(price), 2) AS total_revenue
                FROM bike_rentals_view
                """).show();


        // ─────────────────────────────────────────────────────────
        // EXERCICE 4 — Requêtes d'agrégation
        // ─────────────────────────────────────────────────────────
        printSection("EXERCICE 4 : Agrégations");

        // 4.1 Nombre de locations par station de départ
        System.out.println("── 4.1 Nombre de locations par station de départ ──");
        spark.sql("""
                SELECT start_station,
                       COUNT(*) AS nb_rentals
                FROM bike_rentals_view
                GROUP BY start_station
                ORDER BY nb_rentals DESC
                """).show();

        // 4.2 Durée moyenne par station de départ
        System.out.println("── 4.2 Durée moyenne de location par station de départ ──");
        spark.sql("""
                SELECT start_station,
                       ROUND(AVG(duration_minutes), 2) AS avg_duration_min
                FROM bike_rentals_view
                GROUP BY start_station
                ORDER BY avg_duration_min DESC
                """).show();

        // 4.3 Station avec le plus grand nombre de locations
        System.out.println("── 4.3 Station la plus populaire ──");
        spark.sql("""
                SELECT start_station,
                       COUNT(*) AS nb_rentals
                FROM bike_rentals_view
                GROUP BY start_station
                ORDER BY nb_rentals DESC
                LIMIT 1
                """).show();


        // ─────────────────────────────────────────────────────────
        // EXERCICE 5 — Analyse temporelle
        // ─────────────────────────────────────────────────────────
        printSection("EXERCICE 5 : Analyse temporelle");

        // 5.1 Extraction de l'heure depuis start_time
        System.out.println("── 5.1 Extraction de l'heure (colonne hour_of_day) ──");
        spark.sql("""
                SELECT rental_id,
                       start_time,
                       HOUR(start_time) AS hour_of_day
                FROM bike_rentals_view
                ORDER BY rental_id
                LIMIT 10
                """).show(false);

        // 5.2 Nombre de vélos loués par heure (heures de pointe)
        System.out.println("── 5.2 Nombre de locations par heure (heures de pointe) ──");
        spark.sql("""
                SELECT HOUR(start_time) AS hour_of_day,
                       COUNT(*)          AS nb_rentals
                FROM bike_rentals_view
                GROUP BY HOUR(start_time)
                ORDER BY nb_rentals DESC
                """).show();

        // 5.3 Station la plus populaire le matin (7h–12h)
        System.out.println("── 5.3 Station la plus populaire le matin (7h–12h) ──");
        spark.sql("""
                SELECT start_station,
                       COUNT(*) AS nb_rentals
                FROM bike_rentals_view
                WHERE HOUR(start_time) BETWEEN 7 AND 11
                GROUP BY start_station
                ORDER BY nb_rentals DESC
                LIMIT 1
                """).show();


        // ─────────────────────────────────────────────────────────
        // EXERCICE 6 — Analyse du comportement utilisateur
        // ─────────────────────────────────────────────────────────
        printSection("EXERCICE 6 : Comportement utilisateur");

        // 6.1 Âge moyen des utilisateurs
        System.out.println("── 6.1 Âge moyen des utilisateurs ──");
        spark.sql("""
                SELECT ROUND(AVG(age), 2) AS avg_age
                FROM bike_rentals_view
                """).show();

        // 6.2 Nombre d'utilisateurs par genre
        System.out.println("── 6.2 Nombre de locations par genre ──");
        spark.sql("""
                SELECT gender,
                       COUNT(*) AS nb_rentals
                FROM bike_rentals_view
                GROUP BY gender
                ORDER BY nb_rentals DESC
                """).show();

        // 6.3 Tranche d'âge qui loue le plus
        System.out.println("── 6.3 Locations par tranche d'âge ──");
        spark.sql("""
                SELECT
                    CASE
                        WHEN age BETWEEN 18 AND 30 THEN '18-30'
                        WHEN age BETWEEN 31 AND 40 THEN '31-40'
                        WHEN age BETWEEN 41 AND 50 THEN '41-50'
                        ELSE '51+'
                    END AS age_group,
                    COUNT(*) AS nb_rentals
                FROM bike_rentals_view
                GROUP BY age_group
                ORDER BY nb_rentals DESC
                """).show();


        // ─────────────────────────────────────────────────────────
        // Fin — fermeture de la session Spark
        // ─────────────────────────────────────────────────────────
        printSeparator("TP terminé avec succès !");
        spark.stop();
    }

    // ── Utilitaires d'affichage ──────────────────────────────────

    private static void printSeparator(String title) {
        System.out.println("\n" + "=".repeat(62));
        System.out.println("  " + title);
        System.out.println("=".repeat(62));
    }

    private static void printSection(String title) {
        System.out.println("\n\n### " + title + " ###\n");
    }
}

