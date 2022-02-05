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
      .withColumn("title",col("level1.snippet.title"))
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
  val dfGb = merge(csvToDf(path_to_csv_GB),flattenJson(path_to_json_GB)).withColumn("country",lit("Grand Britain"))

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
  











}

