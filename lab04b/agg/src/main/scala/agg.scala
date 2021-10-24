import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object agg {
  def main(args: Array[String]): Unit = {
    val topic_in = "victor_burimskiy"
    val topic_out = topic_in + "_lab04b_out"
    val host = "spark-master-1:6667"
    val mode = "update"
    val checkpointLocation = "hdfs:///user/victor.burimskiy/lab04b_checkpoint"

    val spark: SparkSession = SparkSession
      .builder()
      .appName("victro_burimskiy_lab04b")
      .getOrCreate()

    import spark.implicits._

    val df = read_stream_from_kafka(spark, host, topic_in, get_schema())
      .withColumn("timestamp", from_unixtime('timestamp / 1000).cast(TimestampType))
      //.withWatermark("timestamp", "5 seconds")
      .groupBy(
        window('timestamp, "60 minutes"))
      .agg(
        sum(when('event_type === "buy", 'item_price).otherwise(0)).as("revenue"),
        sum(when('uid.isNotNull, 1).otherwise(0)).as("visitors"),
        sum(when('event_type === "buy", 1).otherwise(0)).as("purchases")
      )
      .withColumn("aov", 'revenue.cast(DoubleType) / 'purchases.cast(DoubleType))
      .withColumn("start_ts", unix_timestamp(col("window.start")))
      .withColumn("end_ts", unix_timestamp(col("window.end")))
      .withColumn("key", 'start_ts.cast(StringType))
      .withColumn("value", to_json(
        struct("start_ts", "end_ts", "revenue", "visitors", "purchases", "aov")
      ))
      .select("key", "value")

    kafka_stream_writer(df, host, topic_out, checkpointLocation)
      .outputMode(mode)
      .start()
      .awaitTermination()

    spark.stop()
  }

  def get_schema(): StructType = {
    new StructType()
      .add("event_type", StringType)
      .add("category", StringType)
      .add("item_id", StringType)
      .add("item_price", IntegerType)
      .add("uid", StringType)
      .add("timestamp", LongType)
  }

  def read_stream_from_file(spark: SparkSession, schema: StructType, path: String) = {
    spark
      .readStream
      .format("json")
      .schema(schema)
      .option("maxFilesPerTrigger", "1")
      .option("path", path)
      .load
  }

  def read_stream_from_kafka(spark: SparkSession,
                             host: String,
                             topic: String,
                             schema: StructType,
                             maxOffsetsPerTrigger: Int = 18000) = {
    import spark.implicits._

    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", host)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()
      .select(from_json('value.cast(StringType), schema).as("json"))
      .select("json.*")
  }

  def console_stream_writer(df: DataFrame) = {
    df
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("truncate", "false")
      .option("numRows", "200")
  }

  def kafka_stream_writer(df: DataFrame,
                          host: String,
                          topic: String,
                          checkpointLocation: String) = {
    df
      .writeStream
      .format("kafka")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", checkpointLocation)
      .option("kafka.bootstrap.servers", host)
      .option("topic", topic)
  }

  def killAll(): Unit = {
    SparkSession
      .active
      .streams
      .active
      .foreach { x =>
        val desc = x.lastProgress.sources.head.description
        x.stop
        println(s"Stopped $desc")
      }
  }
}
