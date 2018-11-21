package run

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  *
  * @author wangjx
  * 读取ES的表结构信息   ES表需要事先创建
  * name  type:text   age type:integer  country type:keyword
  */
object SparkReadFromES {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkReadFromES").setMaster("local")
    conf.set("es.index.auto.create","true")
    conf.set("es.nodes","127.0.01")
    conf.set("es.port","9200")
    val sc = new SparkContext(conf)
    /**
     *
     * 第一种方式 直接读取 读取的结果 偏移量第一位是id  第二位是所有的map值
     *
     */
//    val rdd = sc.esRDD("people/man","?q=提莫队长")
//    rdd.foreach(rdd =>
//      println(rdd._1+"     "+rdd._2)
//    )
//
    /**
     *
     * 第二种方式 创建sparksession的方式 用sparksql的方式读取ES 读取到的是具体的值（无序）
     *
     */
    val spark = SparkSession.builder()
      .appName("SparkReadFromES")
      .master("local")
      .config("es.index.auto.create", "true")
      .config("es.nodes", "127.0.0.1")
      .config("es.port", "9200")
      .getOrCreate()
    val sparkDF = spark.sqlContext.read.format("org.elasticsearch.spark.sql")
      .load("people/man")
    //sparkDF.foreach(println(_))
    //sparkDF.createOrReplaceGlobalTempView("people")
    sparkDF.createOrReplaceTempView("people")
    var people = spark.sql("select * from people where name='无双剑姬'")

    people.show()
  }
}
