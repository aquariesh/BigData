package run

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * 数据关联入库demo
  */
object TestLocal {

  case class data(city: String, five: Int, four: Int, three: Int, two: Double, one: Double)

  case class ResultWithCityId(city: String, city_id: Int, five: Int, four: Int, three: Int, two: Double, one: Double)

  case class ResultWithOutCityId(city: String, five: Int, four: Int, three: Int, two: Double, one: Double)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("TestLocal")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "127.0.0.1")
    conf.set("es.port", "9200")
    val ssc = new StreamingContext(conf, Seconds(10))
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val Dstream = ssc.socketTextStream("localhost", 9999)
    val stream = Dstream.map(_.split(",")).map(m => (m(0), (m(1).toInt, m(2).toInt, m(3).toInt, m(4).toDouble, m(5).toDouble)))
    val xx = stream.transform(rdd => {
      //读取city的表
      val sparkDF = spark.sqlContext.read.format("org.elasticsearch.spark.sql").load("city/city")
      sparkDF.show()
      val baseRDD = sparkDF.rdd.map(m => {
        val city = m.getAs[String]("city")
        val city_id = m.getAs[Int]("city_id")
        (city, city_id)
      })


      //val joinRDD = rdd.join(baseRDD)
      val joinRDD = rdd.leftOuterJoin(baseRDD)

      val resultRDD = joinRDD.map(m => {
        val resultCity = m._1
        //println(resultCity)
        val resultCityId = m._2._2.getOrElse(0)
        //println(resultCityId)
        val five = m._2._1._1
        val four = m._2._1._2
        val three = m._2._1._3
        val two = m._2._1._4
        val one = m._2._1._5
        if (resultCityId == 0) {
          ResultWithOutCityId(resultCity, five, four, three, two, one)
        }
        else {
          ResultWithCityId(resultCity, resultCityId, five, four, three, two, one)
        }
      })
      resultRDD
    })
    xx.foreachRDD(rdd => {
      //val rdd: RDD[Product with scala.Serializable] = rdd
      EsSpark.saveToEs(rdd, "result/result")
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
