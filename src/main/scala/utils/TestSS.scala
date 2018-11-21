package utils

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

object TestSS {

  case class people(name:String,country:String,age:Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("KafkaToStructuredStreamingToEs")
      .config("es.nodes", "localhost")
      .config("es.port", "9200")
      .config("es.index.auto.create", "true")
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
        $"value"  cast "string"
      ).as[String]
    var name =""
    println(df.schema)
    val result = df.map(x=>{
      val map = x.substring(1, x.length - 1)
        .split(",")
        .map(_.split(":"))
        .map { case Array(k, v) => (k.substring(1, k.length - 1), v.substring(1, v.length - 1)) }
        .toMap[String, Object]
         name = map.get("name").getOrElse("").toString
          println(name+"name")
          name
    })

    println(name+"++++++++++++++++++")
    var index = "people/man"
    name match {
      case "无双剑姬" => {
        index = "people/man"
        println(index+"index")
      }
      case "巨魔之王" => {
        index = "wjx/man"
        println(index+"index")
      }
      case _ => println("other")
    }
    result.writeStream
      .option("checkpointLocation", "/tmp/checkpointLocation")
      .format("es")
      .start(index)
      .awaitTermination()
  }
}
