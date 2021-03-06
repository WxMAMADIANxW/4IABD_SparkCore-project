import org.apache.spark
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{array, explode}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import sun.util.calendar.LocalGregorianCalendar.Date
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, Encoders}
import java.util.Calendar
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.regexp_replace


object init_load_transform extends App{
  /** == Création d'une session spark ==
   *
   */
  val spark = SparkSession.builder()
    .appName("init_load&transform ")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._


  /** == Creation du Schema pour les fichiers CSV ==
   *
   */
  val schema =  new StructType()
   .add("video_id",StringType,false)
   .add("title",StringType,false)
   .add("publishedAt",StringType,false)
   .add("channelId",StringType,false)
   .add("channelTitle",StringType,false)
   .add("categoryId",StringType,false)
   .add("trending_date",StringType,false)
   .add("tags",StringType,false)
   .add("view_count",IntegerType,false)
   .add("likes",IntegerType,false)
   .add("dislikes",IntegerType,false)
   .add("comment_count",IntegerType,false)
   .add("thumbnail_link",StringType,false)
   .add("comments_disabled",StringType,false)
   .add("ratings_disabled",StringType,false)
   .add("description",StringType,false)


  /** == csvToDf() ==
   * Cette fonction lit un csv, supprime les lignes qui contiennent des valeurs null et retourne un DataFrame
   * @param path de type String
   * @return DataFrame
   */
  def csvToDf(path:String): DataFrame={
    val df_video = spark.read.format("csv")
      .option("header","true")
      .option("multiLine", true)
      .schema(schema)
      .load(path)

    df_video.na.drop()
  }

  /** == flattenJson() ==
   * Cette fonction lit un json, l'applatie et retourne le resultat en type DataFrame
   * @param path de type String
   * @return DataFrame
   */
  def flattenJson(path:String): DataFrame={
    val df_categorie = spark.read
      .option("multiLine","true")
      .json(path)

    df_categorie.select(col("kind"),col("etag"), explode(col("items")) as "level1")
      .withColumn("id", col("level1.id"))
      .withColumn("titleCategorie",col("level1.snippet.title"))
      .withColumn("assignable", col("level1.snippet.assignable"))
      .drop("level1")
  }

  /** == merge() ==
   * Merge deux dataframe par la column categoryId
   * @param df1 type DataFrame from csv
   * @param df2 type DataFrame from json
   * @return DataFrame
   */
  def merge(df1 :DataFrame, df2 : DataFrame): DataFrame= {
    df1.join(df2.withColumnRenamed("id","categoryId"), Seq("categoryId")).drop("kind","etag")
  }

  /**
   * =================================================================================================================
   * ====================================  Début création des dataframes par pays ==================================== 
   * =================================================================================================================
   */

  /**Creation DataFrame BR
   *
   */
  val path_to_csv_BR = "data/raw_data/BR_youtube_trending_data.csv"
  val path_to_json_BR = "data/raw_data/BR_category_id.json"
  val dfBr = merge(csvToDf(path_to_csv_BR),flattenJson(path_to_json_BR)).withColumn("country",lit("Brazil"))

  /**Creation DataFrame CA
   *
   */
  val path_to_csv_CA = "data/raw_data/CA_youtube_trending_data.csv"
  val path_to_json_CA = "data/raw_data/CA_category_id.json"
  val dfCa = merge(csvToDf(path_to_csv_CA),flattenJson(path_to_json_CA)).withColumn("country",lit("Canada"))

  /**Creation DataFrame DE
   *
   */
  val path_to_csv_DE = "data/raw_data/DE_youtube_trending_data.csv"
  val path_to_json_DE = "data/raw_data/DE_category_id.json"
  val dfDe = merge(csvToDf(path_to_csv_DE),flattenJson(path_to_json_DE)).withColumn("country",lit("Deutschland"))

  /**Creation DataFrame FR
   *
   */
  val path_to_csv_FR = "data/raw_data/FR_youtube_trending_data.csv"
  val path_to_json_FR = "data/raw_data/FR_category_id.json"
  val dfFr = merge(csvToDf(path_to_csv_FR),flattenJson(path_to_json_FR)).withColumn("country",lit("France"))

  /**Creation DataFrame GB
   *
   */
  val path_to_csv_GB = "data/raw_data/GB_youtube_trending_data.csv"
  val path_to_json_GB = "data/raw_data/GB_category_id.json"
  val dfGb = merge(csvToDf(path_to_csv_GB),flattenJson(path_to_json_GB)).withColumn("country",lit("Great Britain"))

  /**Creation DataFrame IN
   *
   */
  val path_to_csv_IN = "data/raw_data/IN_youtube_trending_data.csv"
  val path_to_json_IN = "data/raw_data/IN_category_id.json"
  val dfIn = merge(csvToDf(path_to_csv_IN),flattenJson(path_to_json_IN)).withColumn("country",lit("India"))

  /**Creation DataFrame JP
   *
   */
  val path_to_csv_JP = "data/raw_data/JP_youtube_trending_data.csv"
  val path_to_json_JP = "data/raw_data/JP_category_id.json"
  val dfJp = merge(csvToDf(path_to_csv_JP),flattenJson(path_to_json_JP)).withColumn("country",lit("Japan"))

  /**Creation DataFrame KR
   *
   */
  val path_to_csv_KR = "data/raw_data/KR_youtube_trending_data.csv"
  val path_to_json_KR = "data/raw_data/KR_category_id.json"
  val dfKr = merge(csvToDf(path_to_csv_KR),flattenJson(path_to_json_KR)).withColumn("country",lit("Korea"))

  /**Creation DataFrame MX
   *
   */
  val path_to_csv_MX = "data/raw_data/MX_youtube_trending_data.csv"
  val path_to_json_MX = "data/raw_data/MX_category_id.json"
  val dfMx = merge(csvToDf(path_to_csv_MX),flattenJson(path_to_json_MX)).withColumn("country",lit("Mexico"))

  /**Creation DataFrame RU
   *
   */
  val path_to_csv_RU = "data/raw_data/RU_youtube_trending_data.csv"
  val path_to_json_RU = "data/raw_data/RU_category_id.json"
  val dfRu = merge(csvToDf(path_to_csv_RU),flattenJson(path_to_json_RU)).withColumn("country",lit("Russia"))

  /**Creation DataFrame US
   *
   */
  val path_to_csv_US = "data/raw_data/US_youtube_trending_data.csv"
  val path_to_json_US = "data/raw_data/US_category_id.json"
  val dfUs = merge(csvToDf(path_to_csv_US),flattenJson(path_to_json_US)).withColumn("country",lit("United State"))

  /**
   * =================================================================================================================
   * ====================================  Fin création des dataframes par pays ====================================
   * =================================================================================================================
   */

  /** == CreateYoutubeDf() ==
   * merge tout les dataframes contenu dans list
    * @param list type Seq[DataFrame]
   * @return DataFrame
   */
  def createYoutubeDf(list : Seq[DataFrame]): DataFrame = {
    list.reduce(_ union _)
  }

  val dfs = Seq(dfUs,dfCa,dfBr,dfRu,dfDe,dfFr,dfGb,dfIn,dfJp,dfKr,dfMx)
  val youtubeDf = createYoutubeDf(dfs).dropDuplicates("video_id")


  /*
  Début des analyses sur la problematique suivante : Comment percer sur Youtube ?

  Quel genre/categorie performe le plus ? // Choisir le genre de video à produire
  - Dataframe avec les colonnes suivantes : categorieTitle, nombre de vues moyen par categorie,
    nombre de likes moyen par categorie, nombre de commentaires moyen par categorie

  Quel pays a les meilleurs statistiques ? // Pour choisir la langue dans laquelle faire ses videos
  - Dataframe avec les colonnes suivantes : pays, nombre de vues, nombre de likes, nombre de commentaires

  Qui sont les top trend youtubeurs ? // Pour prendre exemple sur les meilleurs
  - Dataframe avec les colonnes suivantes : channelTitle, nombre de vus, average(like), average(comments)

  Quel est le meilleur jour pour mettre en ligne une vidéo ? // Soyons stratégiques jusqu'au bout
  - Dataframe avec les colonnes suivantes : jour de la semaine, nombre moyen de vues, de likes et de nombre de commentaires

  Zoom sur la France :
  - Ses 10 vidéos les plus vues dans le top trend
    • Dataframe avec le nom de la chaîne, le titre de la vidéo, le nombre de vues, likes, commentaires et le titre de la catégorie
  - Ses catégories les les regardées
    • Dataframe avec le titre de la catégorie,le nombre moyen de vues, likes et commentaires

   */

  /**
   *================ Début des analyses ================
   */


  val categoryStatsDf = youtubeDf.groupBy("titleCategorie").agg(
    round(avg("view_count")).as("avg_view_count"),
    round(avg("likes")).as("avg_likes"),
    round(avg("comment_count")).as("avg_comment_count")
  ).orderBy(desc("avg_view_count"))

  val countryStatsDf = youtubeDf.groupBy("country").agg(
    sum("view_count").as("total_view_count"),
    sum("likes").as("total_likes"),
    sum("comment_count").as("total_comment_count"),
  ).orderBy(desc("total_view_count"))

  // formatage de la date de sortie de la vidéo
  val dayRegexStatsDf = youtubeDf.withColumn("publishedAt", regexp_replace(youtubeDf("publishedAt"), "T", " "))
  // récupération du jour de la semaine
  val dayWeekStatsDf = dayRegexStatsDf.withColumn("week_day", date_format(col("publishedAt"), "EEEE"))

  val dayStatsDf = dayWeekStatsDf.groupBy("week_day").agg(
    round(avg("view_count")).as("avg_view_count"),
    round(avg("likes")).as("avg_likes"),
    round(avg("comment_count")).as("avg_comment_count")
  ).orderBy(desc("avg_view_count"))

  val topYoutubersDf = youtubeDf.groupBy("channelTitle").agg(
    sum("view_count").as("total_view_count"),
    round(avg("likes")).as("avg_likes_per_videos"),
    sum("comment_count").as("avg_nbr_comment_per_videos"),
  ).orderBy(desc("total_view_count"))

  val franceTop10StatsDf = dfFr.select("channelTitle", "title", "view_count", "likes", "comment_count", "titleCategorie", "country")
    .orderBy(desc("view_count"))
    .limit(10)

  val franceCategoryStatsDf = dfFr.groupBy("titleCategorie").agg(
    round(avg("view_count")).as("avg_view_count"),
    round(avg("likes")).as("avg_likes"),
    round(avg("comment_count")).as("avg_comment_count")
  ).orderBy(desc("avg_view_count")).limit(10)

  println("\nCATÉGORIES À PRIVILÉGIER\n")
  categoryStatsDf.show()

  println("\nPAYS À PRIVILÉGIER\n")
  countryStatsDf.show()

  println("\nJOURS À PRIVILÉGIER\n")
  dayStatsDf.show()

  println("\nLES MEILLEURS YOUTUBERS DANS LE MONDE\n")
  countryStatsDf.show()

  println("\nZOOM FRANCE : LE TOP 10 DES VIDÉOS\n")
  franceTop10StatsDf.show()

  println("\nZOOM FRANCE : LE TOP 10 DES CATÉGORIES\n")
  franceCategoryStatsDf.show()

    /*Spark UI*/
  System.in.read();
  spark.stop();
}

