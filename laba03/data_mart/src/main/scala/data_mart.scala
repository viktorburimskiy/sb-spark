import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.elasticsearch.spark._

object data_mart extends App {

  //Cassandra
  //Информация о клиентах

  val spark = SparkSession
    .builder()
    .appName("Lab03")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.cassandra.connection.host", "10.0.0.5")
  spark.conf.set("spark.cassandra.connection.port", "9042")
  spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
  spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

  val clients = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "clients", "keyspace" -> "labdata"))
    .load
    .select("uid","gender", "age")

  val clientsDF = clients.withColumn("age_cat",
    when(col("age") < 25, "18-24")
      .when(col("age") < 35, "25-34")
      .when(col("age") < 45, "35-44")
      .when(col("age") < 55, "45-54")
      .otherwise(">=55"))
    .drop(col("age"))

  //ElasticSearch
  //Логи посещения интернет-магазина
  val esOptions =
  Map(
    "es.nodes" -> "10.0.0.5:9200",
    "es.batch.write.refresh" -> "false",
    "es.net.http.auth.user" -> "victor.burimskiy",
    "es.net.http.auth.pass" -> "Ksd43FCv",
    "es.nodes.wan.only" -> "true"
  )
  var logShop = spark.read.format("org.elasticsearch.spark.sql").options(esOptions).load("visits*")

  var logShopDF = logShop
    .filter(col("uid").isNotNull)
    .withColumn("category_shop", concat(lit("shop"), lit("_"), lower(regexp_replace(col("category"), "[ -]", "_"))))
    .drop("category", "event_type","item_id", "item_price", "timestamp")

  //Json
  //Логи посещения веб-сайтов
  val path = "hdfs:///labs/laba03/weblogs.json"
  val peopleDF = spark.read.json(path)

  val logs_struct = peopleDF.select(col("uid"),
    explode(col("visits")).alias("visits"))

  val logs = logs_struct.select(col("uid"),
    col("visits")("timestamp").as("timestamp"),
    col("visits")("url").as("url"))

  val logsWebDF = logs
    .withColumn("domain", regexp_extract(col("url"), "https?://(?:www\\.|)([\\w.-]+).*",1))
    .filter(length(col("domain")) > 1)
    .select(col("uid"), col("domain"))

  //Postgres
  val jdbcUrl = "jdbc:postgresql://10.0.0.5/labdata?user=victor_burimskiy&password=Ksd43FCv"

  val pg = spark
    .read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", jdbcUrl)
    .option("dbtable", "domain_cats")
    .load()

  val pgDF = pg
    .withColumn("category_web", concat(lit("web"), lit("_"), lower(regexp_replace(col("category"), "-", "_"))))
    .select(col("category_web"), col("domain"))

  //Agg and Join

  val clientShop = clientsDF
    .join(logShopDF, "uid" :: Nil, "left")

  val webCatDF = logsWebDF
    .join(pgDF, "domain" :: Nil, "inner")
    .drop(col("domain"))

  val clientWeb = clientsDF
    .join(webCatDF, "uid" :: Nil, "left")

  val unionDF = clientShop.union(clientWeb)

  val result = unionDF.groupBy(col("uid"),col("gender"),col("age_cat"))
    .pivot("category_shop")
    .agg(count("*"))
    .drop(col("null"))

  val jdbcUrlSave = "jdbc:postgresql://10.0.0.5/victor_burimskiy?user=victor_burimskiy&password=Ksd43FCv"

  result.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", jdbcUrlSave)
    .option("dbtable", "clients")
    .save()
}
