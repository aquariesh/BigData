package SparkStreaming

import dao.CourseClickCountDao
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
import utils.DateUtils

import scala.collection.mutable.ListBuffer


/**
  * @author wangjx
  * 测试kafka数据进行统计  kafka自身维护offset
  */
object FlumeKafkaTest {
  //定义样例类 与es表对应
  /**
    *
    * @param ip 日志访问的ip地址
    * @param time 日志访问的时间
    * @param courseId 日志访问的实战课程编号
    * @param statusCode 日志访问的状态吗
    * @param referer 日志访问的referer信息
    */
  case class ClickLog(ip:String,time:String,courseId:Int,statusCode:Int,referer:String)

  /**
    * 实战课程点击数
    * @param day_course 对应的就是hbase中的rowkey 20171111_1
    * @param click_count 对应20171111_1的访问总数
    */
  case class CourseClickCount(day_course:String,click_count:Long)
  def main(args: Array[String]): Unit = {
    //spark 配置
    val conf = new SparkConf().setAppName("FlumeKafkaTest").setMaster("local[2]")
    //spark streaming实时计算初始化 定义每10秒一个批次 准实时处理 企业一般都是准实时 比如每隔10秒统计近1分钟的数据等等
    val ssc = new StreamingContext(conf, Seconds(5))
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
    //从kafka获取数据
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )
    //具体的业务逻辑
    val kafkaValue: DStream[String] = stream.flatMap(line=>Some(line.value()))

    val cleanData = kafkaValue.map(line=>{
      val infos = line.split("\t")
      //infos(2)="GET /class/130.html HTTP/1.1"
      //url = /class/130.html
      val url = infos(2).split(" ")(1)
      var courseId = 0

      //把实战课程的课程编号拿到了
      if(url.startsWith("/class")){
        val courseIdHTML = url.split("\\/")(2)
        courseId = courseIdHTML.substring(0,courseIdHTML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0),DateUtils.parseToMinute(infos(1)),courseId,infos(3).toInt,infos(4))
    }).filter(clicklog => clicklog.courseId!=0)

      //统计今天到现在为止实战课程到访问量

      cleanData.map(x=>{
        // hbase rowkey 设计 20171111_88
        (x.time.substring(0,8) + "_" + x.courseId,1)
      }).reduceByKey(_+_).foreachRDD(rdd =>{
        rdd.foreachPartition(partition =>{
          val list = new ListBuffer[FlumeKafkaTest.CourseClickCount]
          partition.foreach(pair =>{
            list.append(FlumeKafkaTest.CourseClickCount(pair._1,pair._2))
          })
          CourseClickCountDao.save(list)
        })
      })






    //启动程序入口
    ssc.start()
    ssc.awaitTermination()
  }
}

