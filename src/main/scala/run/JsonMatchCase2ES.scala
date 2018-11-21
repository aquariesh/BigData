package run

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark


/**
  * @author wangjx
  *         测试kafka数据不同json入不同ES表
  */
object JsonMatchCase2ES extends Serializable {

  case class people(name: String, country: String, age: Int)

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass);
    val conf = new SparkConf().setAppName("SparkStreamingAutoOffsetKafka").setMaster("local[2]")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "127.0.0.1")
    conf.set("es.port", "9200")
    //val session = new SparkSession()
    //val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(10))
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN");
    //ssc.checkpoint("hdfs://x:9000/data")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "x:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "exactly-once",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topic = Set("kafka9")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )

    //    stream.transform(rdd=>{
    //      val lsts = rdd.collect.toList
    //      val list = lsts.map(lst => spark.sparkContext.makeRDD(lst))
    //      rdd
    //    })
    //    def splitRDD(rdd1: org.apache.spark.rdd.RDD[List[(String, String)]]): List[org.apache.spark.rdd.RDD[(String,String)]] ={
    //      val lsts = rdd1.collect.toList
    //      lsts.map(lst => spark.sparkContext.makeRDD(lst))
    //    }
//    var index: String = ""
//    var name:String =""
    stream.foreachRDD(rdd => {
//      var index: String = ""
//      var name:String =""
      //获取json字符串
//      val json = rdd.map(m=>m.value())
//      rdd.foreach(println(_))
//      println(rdd.count())
      if (!rdd.isEmpty()) {
        var index: String = ""
        var name:String =""
        val json = rdd.map(_.value()).toString()
        if(index!=""){
          val map = json.substring(1, json.length - 1)
            .split(",")
            .map(_.split(":"))
            .map { case Array(k, v) => (k.substring(1, k.length - 1), v.substring(1, v.length - 1)) }
            .toMap[String, Object]
            name = map.get("name").getOrElse("").toString
          println(name)
        }
        val js = rdd.map(x => {
          if (name.equals("无双剑姬")) {
            index = "people/man"
          }
          else if (name.equals("巨魔之王")) {
            index = "wjx/man"
          }
          x.value()
        })
        println(name+"name")
        println(index+"index")
        EsSpark.saveJsonToEs(js, index)
      }
    })
      //      //遍历每一条具体的数据
      //      val json = rdd.map(m=>{
      //            val map = m.substring(1, m.length - 1)
      //              .split(",")
      //              .map(_.split(":"))
      //              .map { case Array(k, v) => (k.substring(1, k.length-1), v.substring(1, v.length-1))}
      //              .toMap[String,Object]
      //              println(map+"map")
      //              //得到每条json的name值
      //              val name = map.get("name").getOrElse("").toString
      //              println(name+"name")
      //            (map,name,m)
      //          })

      //        name match {
      //        case "无双剑姬" => println(name+"++++++++++++++++++=")//EsSpark.saveJsonToEs(rdd,"people/man")
      //        case "巨魔之王" => println(name+"-------------------")//EsSpark.saveJsonToEs(rdd,"wjx/man")
      //        case _ => println("other")
      //      }
      //savE
    //    rdd.foreachRDD(rdd=>{
    //      val a =rdd.map(m=>{
    //        m._1
    //      })
    //      val b =rdd.map(m=>{
    //        m._2
    //      })
    //      val c =rdd.map(m=>{
    //        m._3
    //      })
    //
    //    })
    ssc.start()
    ssc.awaitTermination()
  }

}

