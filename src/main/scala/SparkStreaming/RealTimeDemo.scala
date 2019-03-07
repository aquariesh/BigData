package SparkStreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.elasticsearch.spark.rdd.EsSpark
import run.KafkaOffsetToHbase.{people, storeOffset}

/**
  * 实现多种数据源根据tag标签入不同es表功能
  * author wangjx
  * date 2018-11-22
  */
object RealTimeDemo {

  case class ClickLog(ip:String,time:String,course:String,statusCode:Int,referer:String)

  def main(args: Array[String]): Unit = {
    //spark 配置
    val conf = new SparkConf().setAppName("FlumeKafkaTest").setMaster("local[2]")
    //spark streaming实时计算初始化 定义每10秒一个批次 准实时处理 企业一般都是准实时 比如每隔10秒统计近1分钟的数据等等
    val ssc = new StreamingContext(conf, Seconds(5))
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN");
    //设置kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "exactly-once",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //kafka主题
    val topic = Set("kafkaTest")
    val topic2 = "hbaseTest"
    //从kafka获取数据
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )
    //具体的业务逻辑
    val kafkaValue: DStream[String] = stream.flatMap(line=>Some(line.value()))

//    kafkaValue.filter(m=>m.split("\t")(3).toInt==200).foreachRDD(m=>{
//     m.foreach(println(_))
//    })

    val DStream200 = kafkaValue.filter(m=>m.split("\t")(3).toInt==200).foreachRDD(rdd => {
      rdd.foreach(println(_))
      val result = rdd.map(_.split("\t")).map(m => ClickLog(m(0), m(1),m(2),m(3).toInt,m(4)))
      EsSpark.saveToEs(result, "people1/clicklog")
    })

    val DStream404 = kafkaValue.filter(m=>m.split("\t")(3).toInt==404).foreachRDD(rdd => {
      rdd.foreach(println(_))
      val result = rdd.map(_.split("\t")).map(m => ClickLog(m(0), m(1),m(2),m(3).toInt,m(4)))
      EsSpark.saveToEs(result, "people2/clicklog")
    })

    val DStream500 = kafkaValue.filter(m=>m.split("\t")(3).toInt==500).foreachRDD(rdd => {
      rdd.foreach(println(_))
      val result = rdd.map(_.split("\t")).map(m => ClickLog(m(0), m(1),m(2),m(3).toInt,m(4)))
      EsSpark.saveToEs(result, "people3/clicklog")
    })

//    kafkaValue.foreachRDD(rdd => {
//      storeOffset(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, topic2)
//    })
    //启动程序入口
    ssc.start()
    ssc.awaitTermination()
  }
}
