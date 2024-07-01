package task

import com.example.protobuf.message.AggregatedData
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

object Main extends App {

  val spark = SparkSession.builder()
    .appName("Simple Application")
    .config("spark.master", "local[*]")
    .getOrCreate()

  import spark.implicits._

  val dataPath = "./raw_data"
  val userAgent = "some user agent"

  // Load data
  val clicksDF = spark.read.format("parquet").load(s"$dataPath/clicks*").filter(col("device_settings.user_agent") === userAgent)
  val impressionsDF = spark.read.format("parquet").load(s"$dataPath/impressions*").filter(col("device_settings.user_agent") === userAgent)

  // transform timestamps
  val clickCountHours = clicksDF.withColumn("timestamp", from_unixtime(col("transaction_header.creation_time") / 1000)) // Convert milliseconds to seconds
    .withColumn("hour", date_trunc("hour", col("timestamp")))
  val impressionCountHours = impressionsDF.withColumn("timestamp", from_unixtime(col("transaction_header.creation_time") / 1000)) // Convert milliseconds to seconds
    .withColumn("hour", date_trunc("hour", col("timestamp")))

  // calculate counts
  val clicksCounts = clickCountHours.groupBy("hour").agg(sum(size(col("unload.clicks"))).as("clicks_count")).orderBy("hour")
  val impressionCounts = impressionCountHours.groupBy("hour").count.withColumnRenamed("count", "impressions_count").orderBy("hour")

  // Calculate the difference in seconds between subsequent rows
  val windowSpec = Window.orderBy("hour")
  val impressionWithDiff = impressionCounts.withColumn(
    "time_diff",
    unix_timestamp(lead("hour", 1).over(windowSpec)) - unix_timestamp(col("hour"))
  )
  val clicksWithDiff = clicksCounts.withColumn(
    "time_diff",
    unix_timestamp(lead("hour", 1).over(windowSpec)) - unix_timestamp(col("hour"))
  )

  // Convert time_diff from seconds to more suitable units if necessary, e.g., hours
  val impressionWithDiffInHours = impressionWithDiff.withColumn("time_diff_hours", (col("time_diff") / 3600).cast("long")).filter(col("time_diff").isNotNull)
  val clicksWithDiffInHours = clicksWithDiff.withColumn("time_diff_hours", (col("time_diff") / 3600).cast("long")).filter(col("time_diff").isNotNull)


  // Joining the DataFrames on the 'hour' column
  val combinedDF = clicksCounts
    .join(impressionCounts, "hour", "outer")
    .orderBy("hour")

  // Add time_diff
  val diffs = combinedDF.join(clicksWithDiffInHours.select("hour", "time_diff_hours"), Seq("hour"), "left_outer").withColumnRenamed("time_diff_hours", "clicks_time_diff")
    .join(impressionWithDiffInHours.select("hour", "time_diff_hours"), Seq("hour"), "left_outer").withColumnRenamed("time_diff_hours", "impressions_time_diff")
    .na.fill(0)

  // kafka
  val dfWithProto: Dataset[Array[Byte]] = diffs.map(row => {
    AggregatedData(
      hour = row.getAs[java.sql.Timestamp]("hour").getTime,
      clicksCount = row.getAs[Long]("clicks_count"),
      impressionsCount = row.getAs[Long]("impressions_count"),
      clicksTimeDiff = row.getAs[Long]("clicks_time_diff"),
      impressionsTimeDiff = row.getAs[Long]("impressions_time_diff"),
    ).toByteArray
  })

  dfWithProto
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "my-topic")
    .save()


  // check kafka
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092") // Replace with your Kafka broker's address
    .option("subscribe", "my-topic") // Replace with your Kafka topic
    .option("startingOffsets", "earliest") // Use "latest" for only new messages
    .load()
    .map(row => {
      val value: Array[Byte] = row.getAs[Array[Byte]]("value")
      val v = AggregatedData.parseFrom(value).toString
      v
    })

  val query = df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", false)
    .start()

  query.awaitTermination()

  spark.stop()
}