package utils

import scala.collection.JavaConversions._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @author wangjx
 * spark Streaming 消费Kafka数据，offSet 保存在Hbase
 */
object KafkaHbaseCheckPoint {

  // Hbase 简要配置以及开启服务
  val hbaseConf =  HBaseConfiguration.create()

  hbaseConf.set("hbase.zookeeper.quorum", "localhost:2181")
  val connHbase = ConnectionFactory.createConnection(hbaseConf)
  val admin = connHbase.getAdmin()


  // 确认 Hbase 表存在
  def ensureHbaseTBExsit(topic:String) = {

    val tableName = TableName.valueOf("kafka_offset")
    val isExist = admin.tableExists(tableName)

    // 是否存在表，不存在新建
    if (!isExist) {
      val htable = new HTableDescriptor(tableName)
      // topic 为 ColumnFamily
      val column = new HColumnDescriptor(topic)
      column.setMaxVersions(1)
      htable.addFamily(column)
      admin.createTable(htable)
      println("表创建成功:" + htable)
    }

  }

  // 保存新的 OffSet
  def storeOffSet(ranges: Array[OffsetRange], topic:Array[String]) = {

    val table = new HTable(hbaseConf, "kafka_offset")
    table.setAutoFlush(false, false)

    ensureHbaseTBExsit(topic(0).toString)

    var putList:List[Put]= List()
    for(o <- ranges){
      val rddTopic = o.topic
      val rddPartition = o.partition
      val rddOffSet = o.untilOffset
      println("topic:" + rddTopic + ",    partition:" + rddPartition + ",    offset:" + rddOffSet)

      // ColumnFamily
      val put = new Put(Bytes.toBytes("kafka_offSet_" + o.topic))
      put.add(Bytes.toBytes(o.topic), Bytes.toBytes(o.partition), Bytes.toBytes(o.untilOffset))

      putList = put+:putList

    }
    table.put(putList)
    table.flushCommits()
    println("保存新 offset 成功！")
  }


  // 得到历史 OffSet
  def getOffset(topic: Array[String]):(Map[TopicPartition, Long], Int) ={

    val topics = topic(0).toString
    val fromOffSets = scala.collection.mutable.Map[TopicPartition, Long]()

    ensureHbaseTBExsit(topics)


    val table = new HTable(hbaseConf, "kafka_offset")
    val rs = table.getScanner(new Scan())

    // 获取数据  每条数据的列名为partition，值为offset
    for (r:Result <- rs.next(10)) {
      for (kv:KeyValue <- r.raw()) {
        val partition = Bytes.toInt(kv.getQualifier)
        val offSet = Bytes.toLong(kv.getValue)
        println("获取到的partition:" + partition + ",   offset:" + offSet)
        fromOffSets.put(new TopicPartition(topics, partition), offSet)
      }
    }

    // 返回值
    if (fromOffSets.isEmpty){
      (fromOffSets.toMap, 0)
    } else {
      (fromOffSets.toMap, 1)
    }

  }


  // 创建 DStream
  def createMyStreamingContextHbase(ssc:StreamingContext, topic:Array[String],
                                    kafkaParams:Map[String, Object]):InputDStream[ConsumerRecord[String, String]]= {

    var kafkaStreams:InputDStream[ConsumerRecord[String, String]] = null
    val (fromOffSet, flag) = getOffset(topic)

    println("获取到的Offset：" + fromOffSet)

    if (flag == 1) {
      kafkaStreams = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topic, kafkaParams, fromOffSet))
    } else {
      kafkaStreams = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topic, kafkaParams))
    }

    kafkaStreams
  }


  def main(args: Array[String]): Unit = {

    // spark streaming 配置
    val conf = new SparkConf().setAppName("offSet_Hbase").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka 配置
    val brokers = "localhost:9092"
    val topics = Array("hbase")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "hbase_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )


    // StreamingContext
    val lines = createMyStreamingContextHbase(ssc, topics, kafkaParams)

    lines.foreachRDD(rdds => {

      if(!rdds.isEmpty()) {

        println("##################:" + rdds.count())
      }
      //  保存新的 Offset
      storeOffSet(rdds.asInstanceOf[HasOffsetRanges].offsetRanges, topics)

    })

    // 启动
    ssc.start()
    ssc.awaitTermination()

  }

}