package run

import org.apache.spark.sql.{Dataset, SparkSession}
/**
 *
 * @author wangjx
 *  同 KafkaToStructuredStreamingToEs demo
 */
object SimpleStructuredSreaming {

  case class people(name:String,country:String,age:Int)
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      // run app locally utilizing all cores
      .master("local[*]")
      .appName(getClass.getName)
      .config("es.nodes", "localhost")
      .config("es.port", "9200")
      .config("es.index.auto.create", "true") // this will ensure that index is also created on first POST
      .config("es.nodes.wan.only", "true") // needed to run against dockerized ES for local tests
      .getOrCreate()

    import spark.implicits._

    val customerEvents: Dataset[people] = spark
      .readStream
      .format("kafka")
      // maximum number of offsets processed per trigger interval
      .option("maxOffsetsPerTrigger", 100)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "kafka9")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
      .select(
        $"key" cast "string", // deserialize binary key
        $"value" cast "string", // deserialize binary value
        $"topic",
        $"partition",
        $"offset"
      )
      .as[(String, String, String, String, String)]
      // convert kafka value to case class
      .map(x =>  {
      val split = x._2.split(":")
      val age = split(2).toInt
      people(split(0), split(1), age)
    })

    customerEvents
      .writeStream
      .option("checkpointLocation", "/tmp/checkpointLocation")
      //.option("es.mapping.id", "id")
      .format("es")
      .start("people/man")
      .awaitTermination()
  }

}
