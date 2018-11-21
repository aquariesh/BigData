package run

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  *
  * @author wangjx
  *         spark streaming 消费 kafka 消息并把偏移量存入 hbase
  */
object KafkaOffsetToHbase {
  //hbase 配置并开启服务 获取到hbase连接
  val hbaseConf = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.quorum", "localhost:2181")
  val connHbase = ConnectionFactory.createConnection(hbaseConf)
  val admin = connHbase.getAdmin()

  case class people(name: String, country: String, age: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaOffsetToHbase").setMaster("local[2]")
      //设置ES集群地址
      .set("es.index.auto.create", "true")
      .set("es.nodes", "127.0.0.1")
      .set("es.port", "9200")

    val ssc = new StreamingContext(conf, Seconds(5))

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    //spark.sparkContext.setLogLevel("WARN");

    //kafka配置参数
    /**
      * 准备kafka参数
      */
    val topic = "hbaseTest"
    //分区数
    val patition = 3
    val topics = Array("kafkaTest")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "304",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = createKafkaStreams(ssc, topics, kafkaParams)

    //业务逻辑与存储offset
    //      val kafkaValue: DStream[String] = stream.flatMap(line=>Some(line.value()))
    //
    //      kafkaValue.foreachRDD(rdd =>{
    //        val result = rdd.map(_.split(":")).map(m=>people(m(0),m(1),m(2).toInt))
    //        EsSpark.saveToEs(result, "people/man")
    //        storeOffset(rdd.asInstanceOf[HasOffsetRanges].offsetRanges,topic)
    //      })

    stream.foreachRDD(rdd => {
      val kafkaValue = rdd.map(line => line.value())
      val result = kafkaValue.map(_.split(":")).map(m => people(m(0), m(1), m(2).toInt))
      EsSpark.saveToEs(result, "people/man")
      storeOffset(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, topic)
    })

    // 启动
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    *
    * @author wangjx
    *         创建kafka输入流
    */
  def createKafkaStreams(ssc: StreamingContext, topics: Array[String],
                         kafkaParams: Map[String, Object]): InputDStream[ConsumerRecord[String, String]] = {
    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null
    val (fromOffset, flag) = getOffset(topics)
    println("获取到的Offset：" + fromOffset)
    if (flag == 1) {
      kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topics, kafkaParams, fromOffset))
    }
    else {
      kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topics, kafkaParams))
    }
    kafkaStream
  }

  /**
    *
    * @author wangjx
    *         获取hbase里对应topic的偏移量
    */
  def getOffset(topics: Array[String]): (Map[TopicPartition, Long], Int) = {
    val topic = topics(0).toString
    val fromOffset = scala.collection.mutable.Map[TopicPartition, Long]()
    //判断表和列族是否存在 如果不存在 则创建相应的表和以topic为名称的列族名
    ensureHbaseTableExist(topic)
    // val table2 = connHbase.getTable(new TableName("test"))

    val table = new HTable(hbaseConf, "test")
    val rs = table.getScanner(new Scan())
    //获取数据 每条数据的列名为partition value为offset 如果没有 则从0最开始创建
    for (r: Result <- rs.next(100)) {
      for (kv: Cell <- r.rawCells()) {
        val partition = Bytes.toInt(kv.getQualifierArray)
        val offset = Bytes.toLong(kv.getValueArray)
        println("获取到的partition:" + partition + ",   offset:" + offset)
        fromOffset.put(new TopicPartition(topic, partition), offset)
      }
    }
    //返回值  如果是第一次创建 立个flag置为0
    if (fromOffset.isEmpty) {
      (fromOffset.toMap, 0)
    }
    else {
      (fromOffset.toMap, 1)
    }
  }

  /**
    *
    * @author wangjx
    *         存储到hbase里对应topic的偏移量
    */
  def storeOffset(ranges: Array[OffsetRange], topic: String): Unit = {
    val table = new HTable(hbaseConf, "test")
    table.setAutoFlush(false, false)
    ensureHbaseTableExist(topic)
    //创建一个可变的put list
    var putList: List[Put] = List()
    for (o <- ranges) {
      val rddTopic = o.topic
      val rddPartition = o.partition
      val rddFromOffset = o.fromOffset
      val rddUntilOffset = o.untilOffset
      println("topic:" + rddTopic + ",    partition:" + rddPartition + ",    fromOffset:" + rddFromOffset + ",    untilOffset:" + rddUntilOffset)
      val put = new Put(Bytes.toBytes("test_" + o.topic))
      put.addColumn(Bytes.toBytes(o.topic), Bytes.toBytes(o.partition.toString), Bytes.toBytes(o.untilOffset.toString))
      putList = put +: putList
    }
    import scala.collection.JavaConversions._
    table.put(putList)
    table.flushCommits()
    println("保存新 offset 成功！")
  }

  /**
    *
    * @author wangjx
    *         判断hbase里对应的表和列族是否存在  如果不存在 则创建 如果存在 则直接读取内容
    */
  def ensureHbaseTableExist(topic: String) = {
    val tableName = TableName.valueOf("test")
    val isExist = admin.tableExists(tableName)
    //是否存在表  如果不存在 则创建
    if (!isExist) {
      val htable = new HTableDescriptor(tableName)
      //topic 为列族
      val column = new HColumnDescriptor(topic)
      column.setMaxVersions(1)
      htable.addFamily(column)
      admin.createTable(htable)
      println("表创建成功:" + htable)
    }
  }
}
