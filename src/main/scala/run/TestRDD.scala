package run

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wangjx
  *         测试读取hdfs上的文件进行数据统计
  */
object TestRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      //.setMaster("local")
      .setAppName("testRDD")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("file:///Users/wangjx/data/log.txt")
    val line = lines.flatMap(_.split(","))
    val pairs = line.map((_, 1))

    val test = pairs.reduceByKey(_ + _)
      .map(m => (m._2, m._1)).sortByKey()
      .map(m => (m._2, m._1))
    test.foreach(word => {
      println(word._1, word._2)
    })
    println("你好")
    println("王嘉浠")
  }
}
