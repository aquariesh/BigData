package honey

import honey.HoneyOffLine.Flower
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.elasticsearch.spark.rdd.EsSpark

object HoneyRealTimeAnalyses {

  /**
    * 蜜罐实时处理程序
    */
  case class Flower(column1:Double,column2:Double,column3:Double,
                     column4:Double,prediction:Double,flowerType:String)

  case class tempClass (column1:Double,column2:Double,column3:Double, column4:Double)

  def main(args: Array[String]): Unit = {
    //spark 配置
    val conf = new SparkConf().setAppName("FlumeKafkaTest").setMaster("local[2]")
        conf.set("es.index.auto.create", "true")
        conf.set("es.nodes", "127.0.0.1")
        conf.set("es.port", "9200")
    //spark streaming实时计算初始化 定义每5秒一个批次 准实时处理 企业一般都是准实时 比如每隔10秒统计近1分钟的数据等等
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
    val topic = Set("honey")

    //从kafka获取数据
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )
    //具体的业务逻辑 获取到传输的数据
    val kafkaValue: DStream[String] = stream.flatMap(line => Some(line.value()))

    import spark.implicits._

    //加载hdfs上数据模型
    val model = DecisionTreeClassificationModel.load("hdfs://localhost:9000/model")

    //封装特征
    val assembler = new VectorAssembler()
      .setInputCols(Array("c1", "c2", "c3", "c4"))
      .setOutputCol("features")

    kafkaValue.foreachRDD(rdd=>{
      rdd.foreach(m=>println(m))
      val info = rdd.map(m=>m.split(","))
        .map(m=>tempClass(m(0).toDouble,m(1).toDouble,m(2).toDouble,m(3).toDouble))
        .toDF("c1","c2","c3","c4")
      val tempResult = assembler.transform(info)
      val result = model.transform(tempResult)
      result.show(false)
      val finalresult = result.select("c1", "c2", "c3", "c4", "prediction")
        .map(row => {
          var flowerType = ""
          row.getDouble(4) match {
            case 0.0 => flowerType = "Iris-setosa"
            case 1.0 => flowerType = "Iris-versicolor"
            case 2.0 => flowerType = "Iris-virginica"
          }
          (row.getDouble(0),
            row.getDouble(1),
            row.getDouble(2),
            row.getDouble(3),
            row.getDouble(4),
            flowerType)
        }).toDF("c1","c2","c3","c4","prediction","flowerType")
      val saveResult = finalresult.rdd.map(rdd => Flower(rdd.getDouble(0), rdd.getDouble(1), rdd.getDouble(2), rdd.getDouble(3), rdd.getDouble(4), rdd.getString(5)))
      EsSpark.saveToEs(saveResult,"/flower/doc")

    })

    //启动程序入口
    ssc.start()
    ssc.awaitTermination()
  }
}
