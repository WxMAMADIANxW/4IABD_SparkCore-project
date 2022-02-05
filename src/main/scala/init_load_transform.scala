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






  val df_video = spark.read.format("csv")
    .option("header","true")
    .option("multiLine", true)
    .schema(schema)
    .load("/Users/mamadian_djalo/Documents/ESGI/Spark_Core/Projet/4IABD_SparkCore-project/data/raw_data/BR_youtube_trending_data.csv")
  println("================ Video =====================")
  df_video.na.drop().show()

  println("================ Categorie =====================")

  val df_categorie = spark.read
    .option("multiLine","true")
    .json("/Users/mamadian_djalo/Documents/ESGI/Spark_Core/Projet/4IABD_SparkCore-project/data/raw_data/BR_category_id.json")

  println("-----------PrintSchema Original---------------")
  df_categorie.printSchema()

  val df_categorie_unnested = df_categorie.select(col("kind"),col("etag"), explode(col("items")) as "level1")
    .withColumn("id", col("level1.id"))
    .withColumn("title",col("level1.snippet.title"))
    .withColumn("assignable", col("level1.snippet.assignable"))
    .drop("level1")

  
  /*df_categorie_unnested.select(col("kind"),col("etag"))
    .withColumn("id", explode(col("id")))
    .withColumn("title",explode(col("title")))
    .show(false)*/





  //val df_category_br =

  /* Charge le csv et le json
  *  Clean le csv
  *  merge les video csv et les categ json avec  join
  *
  * */


  /*def flattenJson(path:StringType): DataFrame={
    val df_categorie = spark.read
      .option("multiLine","true")
      .json("/Users/mamadian_djalo/Documents/ESGI/Spark_Core/Projet/4IABD_SparkCore-project/data/raw_data/BR_category_id.json")

    df_categorie.drop("kind","etag")
  }
  def createDf (path: StringType): DataFrame= {
    val df = spark.read.format("csv")
      .option("header","true")
      .option("multiLine", true)
      .schema(schema)
      .load("/Users/mamadian_djalo/Documents/ESGI/Spark_Core/Projet/4IABD_SparkCore-project/data/raw_data/BR_youtube_trending_data.csv")

    df.na.drop().show()


  df_categorie.select(col("kind"),col("etag"), explode(array("items")) as "level1")
    .withColumn("id", col("level1.id"))
    .withColumn("title",col("level1.snippet.title"))
    .withColumn("assignable", col("level1.snippet.assignable"))
    .drop("level1").show(false)

  }*/


}

