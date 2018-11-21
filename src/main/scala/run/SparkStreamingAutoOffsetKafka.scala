package run

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark


/**
  * @author wangjx
  * 测试kafka数据进行统计  kafka自身维护offset（建议使用自定义维护方式维护偏移量）
  */
object SparkStreamingAutoOffsetKafka {
  //定义样例类 与es表对应
  case class people(name:String,country:String,age:Int)

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass);
    //spark 配置
    val conf = new SparkConf().setAppName("SparkStreamingAutoOffsetKafka").setMaster("local[2]")
    conf.set("es.index.auto.create","true")
    conf.set("es.nodes","127.0.0.1")
    conf.set("es.port","9200")
    //spark streaming实时计算初始化 定义每10秒一个批次 准实时处理 企业一般都是准实时 比如每隔10秒统计近1分钟的数据等等
    val ssc = new StreamingContext(conf, Seconds(10))
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    //spark.sparkContext.setLogLevel("WARN");
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
    val topic = Set("kafka8")
    //从kafka获取数据
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )
    //具体的业务逻辑
    val kafkaValue: DStream[String] = stream.flatMap(line=>Some(line.value()))
    val peopleStream = kafkaValue
      .map(_.split(":"))
      //形成people样例对象
      .map(m=>people(m(0),m(1),m(2).toInt))
    //存入ES
    peopleStream.foreachRDD(rdd =>{
      EsSpark.saveToEs(rdd, "people/man")
    })
    //启动程序入口
    ssc.start()
    ssc.awaitTermination()
  }
}
