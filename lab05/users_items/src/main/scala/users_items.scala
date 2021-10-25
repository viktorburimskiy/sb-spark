import java.io.File
import java.nio.file.Paths

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, DataFrame}
import org.apache.spark.sql.functions._

import scala.util.Try
import scala.util.matching.Regex

object users_items {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("victro_burimskiy_lab05")
      .getOrCreate()

    import spark.implicits._

    val update = spark.conf.get("spark.users_items.update", "1").toInt
    val output_dir = spark.conf.get("spark.users_items.output_dir", "users-items")
    val input_dir = spark.conf.get("spark.users_items.input_dir", "visits")

    val buy = spark
      .read
      .format("json")
      .schema(get_schema())
      .option("path", resolve_path(input_dir, "buy"))
      .load
    val view = spark
      .read
      .format("json")
      .schema(get_schema())
      .option("path", resolve_path(input_dir, "view"))
      .load

    val vec = buy.union(view)
      .filter('uid.isNotNull)
      .withColumn("event_type_item_id", concat('event_type,
          lit("_"),
          regexp_replace(lower('item_id), "[- ]", "_")
        )
      )
      .withColumn("count", lit(1))
      .select("uid", "event_type_item_id", "count", "date")
      .persist()

    val date = vec.select(max("date")).head().getString(0)

    var old_vec = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], vec.schema)
    if (update == 1) {
      val old_date = get_max_sub_dir_name(output_dir)
      if (old_date.isSuccess) {
        val old_matrix = spark
          .read
          .format("parquet")
          .schema(get_schema())
          .option("path", resolve_path(output_dir, old_date.get))
          .load
        old_vec = get_vector(spark, old_matrix, old_date.get, get_unpivot_expr(old_matrix))
      }
    }

    get_matrix(spark, vec.union(old_vec))
      //.repartition(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .option("path", resolve_path(output_dir, date))
      .option("maxRecordsPerFile", 200000)
      .save()

    vec.unpersist()
    spark.stop()
  }

  def get_matrix(spark: SparkSession, vec: DataFrame): DataFrame = {
    vec
      .groupBy("uid")
      .pivot("event_type_item_id")
      .sum("count")
      .na.fill(0)
  }

  def get_vector(spark: SparkSession, matrix: DataFrame, date: String, unpivot_expr: String): DataFrame = {
    import spark.implicits._
    matrix.select('uid, expr(unpivot_expr))
      .filter('count =!= 0)
      .withColumn("date", lit(date))
  }

  def get_unpivot_expr(matrix: DataFrame): String = {
    var fields: List[String] = Nil
    var name: String = ""
    for (i <- matrix.schema.fields.indices) {
      name = matrix.schema.fields(i).name
      if (name != "uid")
        fields = s"'$name',$name" :: fields
    }
    s"stack(${fields.length},${fields.mkString(",")}) as (event_type_item_id,count)"
  }

  def resolve_path(prefix: String, dir: String): String = {
    prefix match {
      case abs if abs.startsWith("/") => Paths.get(prefix).resolve(dir).toString
      case withSchema if withSchema.startsWith("file://") || withSchema.startsWith("hdfs://") =>
        val schema = withSchema.substring(0, 7)
        s"$schema${Paths.get(withSchema.replace(schema, "")).resolve(dir).toString}"
      case _ => s"hdfs:///user/victor.burimskiy/${Paths.get(prefix).resolve(dir).toString}"
    }
  }

  def get_max_sub_dir_name(dir: String): Try[String] = {
    val dir_name: Regex = "\\d{8}".r
    Try(new File(dir)
      .listFiles.filter(_.isDirectory)
      .filter(n => dir_name.findFirstMatchIn(n.getName) match {
        case Some(_) => true
        case None => false
      })
      .max
      .getName
    )
  }

  def get_schema(): StructType = {
    new StructType()
      .add("event_type", StringType)
      .add("category", StringType)
      .add("item_id", StringType)
      .add("item_price", IntegerType)
      .add("uid", StringType)
      .add("timestamp", LongType)
      .add("date", StringType)
      .add("_date", StringType)
  }
}
