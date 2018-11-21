package run

import com.mysql.jdbc.log.LogUtils
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.Json
import utils.LoggerUtil


/**
  *
  * @author wangjx
  * flume采集数据sink 到kafka, Structured Streaming实时消费 并存到已存在的ES表
  * kafka10 用来测试
  */

object KafkaToStructuredStreamingToEs {

  case class people(name:String,country:String,age:Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("KafkaToStructuredStreamingToEs")
      .config("es.nodes", "localhost")
      .config("es.port", "9200")
      // this will ensure that index is also created on first POST
      .config("es.index.auto.create", "true")
      // needed to run against dockerized ES for local tests
      //.config("es.nodes.wan.only", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val topic = "kafka10"

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe",topic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
      .select(
  //      $"key" cast "string",
        $"value"  cast "string"//,
  //      $"topic",
  //      $"partition",
  //      $"offset"
      ).as[String]
    //.as[(String,String,String,String,String)]
   // df.printSchema()

      val result = df.map(x=>{
        val column=x.split(":")
        people(column(0),column(1),column(2).toInt)
      })

//    LoggerUtil.error("result",result.schema)
//    result.printSchema()
//      val rdd = df.map(x =>{
//        val column = x._2.split(":")
//        people(column(0),column(1),column(2).toInt)
//      })

    result.writeStream
      .option("checkpointLocation", "/tmp/checkpointLocation")
      .format("es")
      .start("people/man")
      .awaitTermination()

  }
}
