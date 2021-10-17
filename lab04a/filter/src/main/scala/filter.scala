import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object filter {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("lab04a")
      .getOrCreate()

    val offset = spark.conf.get("spark.filter.offset")
    val topic = spark.conf.get("spark.filter.topic_name")
    val dir = spark.conf.get("spark.filter.output_dir_prefix")

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> topic,
      "startingOffsets" -> offset
    )
    val kafka = spark.read.format("kafka").options(kafkaParams).load

    val kafka_json = kafka.select(col("value").cast("string"))

    val df: DataFrame = spark.read.json(kafka_json.toJSON)

    val df_all = df.select(col("*") ,date_format(to_date(from_unixtime(col("timestamp") / 1000), "yyyy-MM-dd"), "yyyyMMdd").as("date"))
    val df_buy = df_all.filter(col("event_type") === "buy")
    val df_view = df_all.filter(col("event_type") === "view")

    df_view.write.format("json").mode("overwrite").partitionBy("date").save(dir + "/view")
    df_buy.write.format("json").mode("overwrite").partitionBy("date").save(dir + "/buy")

    spark.stop()
  }
}