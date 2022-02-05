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
   * @return df_video de type DataFrame
   */
  def csvToDf(path:String): DataFrame={
    val df_video = spark.read.format("csv")
      .option("header","true")
      .option("multiLine", true)
      .schema(schema)
      .load("/data/raw_data/BR_youtube_trending_data.csv")

    df_video.na.drop()
  }

  /** == flattenJson() ==
   * Cette fonction lit un json, l'applatie et retourne le resultat en type DataFrame
   * @param path de type String
   * @return df_categorie de type DataFrame
   */
  def flattenJson(path:String): DataFrame={
    val df_categorie = spark.read
      .option("multiLine","true")
      .json(path)

    val df_categorie_unnested = df_categorie.select(col("kind"),col("etag"), explode(col("items")) as "level1")
      .withColumn("id", col("level1.id"))
      .withColumn("title",col("level1.snippet.title"))
      .withColumn("assignable", col("level1.snippet.assignable"))
      .drop("level1")

    df_categorie_unnested
  }

  val path_to_csv = "data/raw_data/BR_youtube_trending_data.csv"
  val path_to_json = "data/raw_data/BR_category_id.json"








}

