package run

import java.util

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import utils.ZkWork

import scala.collection.mutable

/**
  *
  * @author wangjx
  *         StructuredStreaming 手动维护kafka offset 并保存到zookeeper
  *         目前StructuredStreaming支持内置保存偏移量
  *         目前此demo可不用
  */
object StructuredStreamingHandleOffsetKafka {

  val zk = ZkWork

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("StructuredStreamingHandleOffsetKafka")
      .getOrCreate()

    val topic = "kafka7"
    val patition = 3

    import spark.implicits._

    /**
      * 判断zk中是否有保存过该计算的偏移量
      * 如果没有保存过,创建该topic在ZK中的地址,在计算完后保存
      */

    //创建一个分区的offset的list 依次插入分区offset
    var list: util.LinkedList[Long] = new util.LinkedList

    val stream = if (zk.znodeIsExists(s"${topic}offset")) {

      for (i <- 0 until 3) {
        val nor = zk.znodeDataGet(s"${topic}offset/${i}")
        println(s"[ ownOffsetKafka ] --------------------------------------------------------------------")
        println(s"[ ownOffsetKafka ] topic ${topic}")
        println(s"[ ownOffsetKafka ] Partition ${i}")
        println(s"[ ownOffsetKafka ] offset ${nor(2).toLong}")
        println(s"[ ownOffsetKafka ] --------------------------------------------------------------------")

        list.add(nor(2).toLong)
      }

      //一定要是readStream 才能才out是编译
      spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "x:9092")
        .option("subscribe", topic)
        .option("startingOffsets", s"""{"${topic}":{"0":${list.get(0)},"1":${list.get(1)},"2":${list.get(2)}}}""")
        //.option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")
        .load()

    }
    else {

      zk.znodeCreate(s"${topic}offset", s"${topic}offset")

      println(s"[ ownOffsetKafka ] --------------------------------------------------------------------")
      println(s"[ ownOffsetKafka ] 第一次计算,没有zk偏移量文件")
      println(s"[ ownOffsetKafka ] 手动创建一个偏移量文件 ${topic}offset 默认从0偏移度开始计算")
      println(s"[ ownOffsetKafka ] --------------------------------------------------------------------")

      for (i <- 0 until patition) {
        zk.znodeCreate(s"${topic}offset/${i}", s"${topic},${i},0")
      }
      //一定要是readStream 才能才out是编译
      spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "x:9092")
        .option("subscribe", topic)
        .option("startingOffsets", s"""{"${topic}":{"0":0,"1":0,"2":0}}""")
        //        .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")
        .load()

    }

    /**
      * 业务代码部分
      * 将流中的值取出来,用于计算
      */
    //    stream.printSchema()
    //
    //    val schema = StructType(mutable.Seq(
    //      StructField("a", DataTypes.StringType),
    //      StructField("b", DataTypes.StringType),
    //      StructField("c", DataTypes.StringType)
    //    ))
    //
    //    val jsonDF = stream.selectExpr("CAST(key as string)", "CAST (value as string)")
    //      //解析json格式  并把value重命名为data
    //      .select(from_json($"value", schema = schema).as("data"))
    //
    //    jsonDF.select("data.*").createOrReplaceTempView("people")
    //
    //    val sql = "select * from people"
    //
    //    val frame: DataFrame = spark.sql(sql)
    //
    //    frame.printSchema()
    //    //在控制台监控
    //    val query = frame.writeStream
    //      //complete,append,update 目前只支持前面两种
    //      .outputMode("append")
    //      //console,parquet,memory,foreach 四种
    //      .format("console")
    //      .start()

    val words = stream.selectExpr("CAST (value as string)").as[String]
      .flatMap(_.split(" "))

    words.printSchema()

    val wordCounts = words.groupBy("value").count()

    wordCounts.printSchema()

    /**
      * 保存偏移度部分
      * （如果在计算的时候失败了，会接着上一次偏移度进行重算，不保存新的偏移度）
      * 计算成功后保存偏移度
      */

        println(1)

        stream.rdd.foreachPartition {
          val offsetRanges = stream.rdd.asInstanceOf[HasOffsetRanges].offsetRanges
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

    val query = wordCounts.writeStream
      // append 追加模式 则是当前输入做数据处理
      .outputMode("complete")
      .format("console").start()
    query.awaitTermination()
  }
}