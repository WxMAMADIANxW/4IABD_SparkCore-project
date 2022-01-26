import org.apache.spark
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import sun.util.calendar.LocalGregorianCalendar.Date

import java.util.Calendar



object init_load_transform extends App{
  /** == Création d'une session spark ==
   *
   */
  val spark = SparkSession.builder()
    .appName("init_load&transform ")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

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
  df_video.na.drop().show()



  //val df_category_br =

  /* Charge le csv et le json
  *  Clean le csv
  *  merge les video csv et les categ json avec  join
  *
  * */
}