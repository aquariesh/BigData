package run

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import utils.ZkWork

/**
  *
  * @author wangjx
  * 自定义维护kafka offset 保存到zookeeper
  */
object SparkStreamingHandleOffsetKafka {

  val zk = ZkWork

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ownOffsetKafka").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //控制控制台打印输出日志级别
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc,Seconds(10))

    /**
      * 准备kafka参数
      */
    val topic = "kafka"
    //分区数
    val patition = 3
    val topics = Array("kafka")
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
      * 精髓就在于KafkaUtils.createDirectStream这个地方
      * 默认是KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))，不加偏移度参数
      * 实在找不到办法，最后啃了下源码。发现可以使用偏移度参数
      */

    var newOffset: Map[TopicPartition, Long] = Map()

    val stream = if (zk.znodeIsExists(s"${topic}offset")){

      for (i <- 0 until patition) {
        //zk.znodeDataGet(s"${topic}offset/${i}")
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
    }
      //assign策略为指定固定的分区集合
      //Assign[String, String](newOffset.keys.toList, kafkaParams, newOffset))

    /**
      * 业务代码部分
      * 将流中的值取出来,用于计算
      */
    val result = stream.flatMap(line=>Some(line.value()))
    result.flatMap(_.split(",")).map(word => (word, 1)).reduceByKey(_ + _).print()

    /**
      * 保存偏移度部分
      * （如果在计算的时候失败了，会接着上一次偏移度进行重算，不保存新的偏移度）
      * 计算成功后保存偏移度
      */

    stream.foreachRDD{
      rdd =>
        //形成offsetRanges对象的数组
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition {
          iter =>
            val o: OffsetRange = offsetRanges(TaskContext.getPartitionId())
            println(s"[ ownOffsetKafka ] --------------------------------------------------------------------")
            println(s"[ ownOffsetKafka ]  topic: ${o.topic}")
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
