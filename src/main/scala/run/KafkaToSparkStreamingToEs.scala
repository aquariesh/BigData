package run

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import run.SparkStreamingHandleOffsetKafka.zk

import scala.collection.mutable
import org.elasticsearch.spark.streaming._

/**
 *
 * @author wangjx
 * flume采集数据sink 到kafka, spark streaming实时消费 并存到已存在的ES表
 */
object KafkaToSparkStreamingToEs {

  case class people(name:String,country:String,age:Int)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("KafkaToSparkStreamingToEs")
      //设置ES集群地址
      .set("es.index.auto.create","true")
    .set("es.nodes","127.0.0.1")
    .set("es.port","9200")

    val ssc = new StreamingContext(conf,Seconds(10))

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setLogLevel("WARN");
    /**
      * 准备kafka参数
      */
    val topic = "kafka8"
    //分区数
    val patition = 3
    val topics = Array("kafka8")
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "304",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /**
      * 判断zk中是否有保存过该计算的偏移量
      * 如果没有保存过,创建该topic在ZK中的地址,在计算完后保存
      */

    var newOffset: Map[TopicPartition, Long] = Map()

    val stream = if (zk.znodeIsExists(s"${topic}offset")){

      for (i <- 0 until patition) {
        val nor = zk.znodeDataGet(s"${topic}offset/${i}")
        val tp = new TopicPartition(topic,i)
        newOffset += (tp -> nor(2).toLong)
        println(s"[ ownOffsetKafka ] --------------------------------------------------------------------")
        println(s"[ ownOffsetKafka ] topic ${topic}")
        println(s"[ ownOffsetKafka ] Partition ${i}")
        println(s"[ ownOffsetKafka ] offset ${nor(2).toLong}")
        println(s"[ ownOffsetKafka ] zk中取出来的kafka偏移量★★★ ${newOffset}")
        println(s"[ ownOffsetKafka ] --------------------------------------------------------------------")
      }

      KafkaUtils.createDirectStream[String, String](ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams, newOffset))
      //assign策略为指定固定的分区集合
      //Assign[String, String](newOffset.keys.toList, kafkaParams, newOffset))
    }
    else {

      zk.znodeCreate(s"${topic}offset",s"${topic}offset")

      println(s"[ ownOffsetKafka ] --------------------------------------------------------------------")
      println(s"[ ownOffsetKafka ] 第一次计算,没有zk偏移量文件")
      println(s"[ ownOffsetKafka ] 手动创建一个偏移量文件 ${topic}offset 默认从0偏移度开始计算")
      println(s"[ ownOffsetKafka ] --------------------------------------------------------------------")

      for (i <- 0 until patition) {
        zk.znodeCreate(s"${topic}offset/${i}",s"${topic},${i},0")
        val tp = new TopicPartition(topic, i)
        newOffset += (tp -> 0L)
      }

      KafkaUtils.createDirectStream[String,String](ssc,
        PreferConsistent,
        Subscribe[String,String](topics,kafkaParams,newOffset))
      //assign策略为指定固定的分区集合
      //Assign[String, String](newOffset.keys.toList, kafkaParams, newOffset))
    }

    val kafkaValue: DStream[String] = stream.flatMap(line=>Some(line.value()))


    val peopleStream = kafkaValue
      .map(_.split(":"))
      //形成people样例对象
      .map(m=>people(m(0),m(1),m(2).toInt))

//    kafkaValue.foreachRDD(rdd =>{
//       val result = rdd.map(_.split(":"))
//        //形成people样例对象
//        .map(m=>people(m(0),m(1),m(2).toInt))
//      val microbatches = mutable.Queue(result)
//      val dstream = ssc.queueStream(microbatches)
//      println("+++++++++++++")
//      EsSparkStreaming.saveToEs(dstream, "people/man")
//    })

    //存入ES
    peopleStream.foreachRDD(rdd =>{
      EsSpark.saveToEs(rdd, "people/man")
      //EsSpark.saveJsonToEs(rdd,"people/man")
    })
      //json格式存入ES
//    kafkaValue.foreachRDD(rdd =>{
//      EsSpark.saveJsonToEs(rdd,"people/man")
//    })

    stream.foreachRDD{
      rdd =>

        //形成offsetRanges对象的数组
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition {
          iter =>
            val o: OffsetRange = offsetRanges(TaskContext.getPartitionId())
            println(s"[ ownOffsetKafka ] --------------------------------------------------------------------")
            //println(s"[ ownOffsetKafka ]  topic: ${o.topic}")
            println(s"[ ownOffsetKafka ]  partition: ${o.partition} ")
            println(s"[ ownOffsetKafka ]  fromOffset 开始偏移量: ${o.fromOffset} ")
            println(s"[ ownOffsetKafka ]  untilOffset 结束偏移量: ${o.untilOffset} 需要保存的偏移量,供下次读取使用★★★")
            println(s"[ ownOffsetKafka ] --------------------------------------------------------------------")

            zk.offsetWork(s"${o.topic}offset/${o.partition}", s"${o.topic},${o.partition},${o.untilOffset}")
        }
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
