import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

object init_load_transform extends App{
  /***
   * Création de la session spark
   *
   */
  val spark = SparkSession.builder()
    .appName("init_load&transform ")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  /***
   *
   */
}
