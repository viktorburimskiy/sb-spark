import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object filter {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("lab04a")
      .getOrCreate()

    import spark.implicits._

    var offset = spark.conf.get("spark.filter.offset")
    val topic = spark.conf.get("spark.filter.topic_name")
    val dir = spark.conf.get("spark.filter.output_dir_prefix")

    if (offset != "ealiest") {
      offset = s"""{"$topic":{"0":$offset}}"""
    }

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> topic,
      "startingOffsets" -> offset
    )
    val kafka = spark.read.format("kafka").options(kafkaParams).load
    val kafka_json = kafka.select(col("value").cast("string")).rdd
    val kafka_st = kafka_json.map(_.mkString(","))
    val df: DataFrame = spark.read.json(kafka_st.toDS())

    //to_utc_timestamp( to_timestamp(col("timestamp")/ 1000), "UTC")
    //from_unixtime(col("timestamp") / 1000)
    val df_all = df.withColumn("date", date_format(to_utc_timestamp(to_timestamp(col("timestamp")/ 1000), "UTC"), "yyyyMMdd"))
    //val df_all = df.select(col("*") ,date_format(to_date(to_utc_timestamp( to_timestamp(col("timestamp")/ 1000), "UTC"), "yyyy-MM-dd"), "yyyyMMdd").as("date"))
    val df_buy = df_all.filter(col("event_type") === "buy")
    val df_view = df_all.filter(col("event_type") === "view")

    df_view.write.format("json").mode("overwrite").partitionBy("date").save(dir + "/view")
    df_buy.write.format("json").mode("overwrite").partitionBy("date").save(dir + "/buy")

    spark.stop()
  }
}