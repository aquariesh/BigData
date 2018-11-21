package run

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import utils.LoggerUtil
/**
 *
 * @author wangjx
 * spark save to ES   ES表需要事先创建
  * name  type:text   age type:integer  country type:keyword
 */
object SparkSave2ES {

  case class people(name:String,age:Integer,country:String)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSave2ES").setMaster("local")
    conf.set("es.index.auto.create","true")
    conf.set("es.nodes","127.0.0.1")
    conf.set("es.port","9200")
    val sc = new SparkContext(conf)

    /**
     *
     * 第一种方式 直接创建数据源插入
     *
     */
//    val number1 = Map("name"->"王狗狗","age"->25,"country"->"japan")
//    val number2 = Map("name"->"何猪猪","age"->22,"country"->"german")
//    sc.makeRDD(Seq(number1,number2)).saveToEs("people/man")
    /**
     *
     * 第二种方式 创建一个跟ES对应表相同的case class进行对象化插入
     *
     */
//     val kate= people("卡特琳娜",10,"诺克萨斯")
//     val gailun = people("盖伦",12,"德玛西亚")
//     val rdd = sc.makeRDD(Seq(kate,gailun))
//      EsSpark.saveToEs(rdd,"/people/man")
    /**
     *
     * 第三种方式 现有的json存入ES
     *
     */
    val json1 = """{"name":"巨魔之王","age":"26","country":"班德尔城"}"""
    val json2 = """{"name":"无双剑姬","age":"22","country":"战争学院"}"""
    LoggerUtil.error("json1",json1)
    LoggerUtil.error("json2",json2)
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    sc.makeRDD(Seq(json1,json2)).saveJsonToEs("people/man")
  }
}
