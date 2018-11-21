package run

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
    *
    * @author wangjx
    * 使用StructuredStreaming读取nc服务器数据 优化于sparkstreaming方式读取
    */
object StructuredStreamingNCTest {
     def main(args: Array[String]): Unit = {
       val spark = SparkSession
         .builder()
         .config(new SparkConf().setAppName("StructuredStreamingNCTest").setMaster("local[2]"))
         .getOrCreate()
       // df to ds
       import spark.implicits._

       val lines = spark.readStream
         .format("socket")
         .option("host","localhost")
         .option("port",7777)
         .load()

       //lines.printSchema()
       val words = lines.as[String].flatMap(_.split(" "))

       //words.printSchema()

       val wordCounts = words.groupBy("value").count()

       //wordCounts.printSchema()
       val query = wordCounts.writeStream
         // append 追加模式 则是当前输入做数据处理
         .outputMode("complete")
         .format("console")
         .start()

       query.awaitTermination()
     }
}
