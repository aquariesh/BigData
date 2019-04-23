package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
/**
  *
  * @author wangjx 
  *  get_json_object()   from_json()   to_json()   explode()   selectExpr()
  *  get_json_object 是抽取出复杂嵌套的几个字段 扁平化 重新组成json   ==>df
  *  from_json 是根据schema 抽取出单独列  ==> ds
  *  to_json 是把所有的组成一个json   ==>ds
  *  selectExpr  查询dataset中指定列等  ==>ds
  *  Explode为给定的map的每一个元素创建一个新的行。比如上面准备的数据，source就是一个map结构。Map中的每一个key/value对都会是一个独立的行。
  *
  */
object JsonTestOne {

  case class DeviceData (id: Int, device: String)


    def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setMaster("local").setAppName("JsonTest")
      val spark = SparkSession.builder()
        .config(conf)
        .getOrCreate()

      val jsonSchema = new StructType()
        .add("battery_level", LongType)
        .add("c02_level", LongType)
        .add("cca3", StringType)
        .add("cn", StringType)
        .add("device_id", LongType)
        .add("device_type", StringType)
        .add("signal", LongType)
        .add("ip", StringType)
        .add("temp", LongType)
        .add("timestamp", TimestampType)

      import spark.implicits._
      //ds
      val eventsDS = Seq (
        (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
        (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""),
        (2, """{"device_id": 2, "device_type": "sensor-ipad", "ip": "88.36.5.1", "cca3": "ITA", "cn": "Italy", "temp": 18, "signal": 25, "battery_level": 5, "c02_level": 1372, "timestamp" :1475600500 }"""),
        (3, """{"device_id": 3, "device_type": "sensor-inest", "ip": "66.39.173.154", "cca3": "USA", "cn": "United States", "temp": 47, "signal": 12, "battery_level": 1, "c02_level": 1447, "timestamp" :1475600502 }"""),
        (4, """{"device_id": 4, "device_type": "sensor-ipad", "ip": "203.82.41.9", "cca3": "PHL", "cn": "Philippines", "temp": 29, "signal": 11, "battery_level": 0, "c02_level": 983, "timestamp" :1475600504 }"""),
        (5, """{"device_id": 5, "device_type": "sensor-istick", "ip": "204.116.105.67", "cca3": "USA", "cn": "United States", "temp": 50, "signal": 16, "battery_level": 8, "c02_level": 1574, "timestamp" :1475600506 }"""),
        (6, """{"device_id": 6, "device_type": "sensor-ipad", "ip": "220.173.179.1", "cca3": "CHN", "cn": "China", "temp": 21, "signal": 18, "battery_level": 9, "c02_level": 1249, "timestamp" :1475600508 }"""),
        (7, """{"device_id": 7, "device_type": "sensor-ipad", "ip": "118.23.68.227", "cca3": "JPN", "cn": "Japan", "temp": 27, "signal": 15, "battery_level": 0, "c02_level": 1531, "timestamp" :1475600512 }"""),
        (8 ,""" {"device_id": 8, "device_type": "sensor-inest", "ip": "208.109.163.218", "cca3": "USA", "cn": "United States", "temp": 40, "signal": 16, "battery_level": 9, "c02_level": 1208, "timestamp" :1475600514 }"""),
        (9,"""{"device_id": 9, "device_type": "sensor-ipad", "ip": "88.213.191.34", "cca3": "ITA", "cn": "Italy", "temp": 19, "signal": 11, "battery_level": 0, "c02_level": 1171, "timestamp" :1475600516 }"""),
        (10,"""{"device_id": 10, "device_type": "sensor-igauge", "ip": "68.28.91.22", "cca3": "USA", "cn": "United States", "temp": 32, "signal": 26, "battery_level": 7, "c02_level": 886, "timestamp" :1475600518 }"""),
        (11,"""{"device_id": 11, "device_type": "sensor-ipad", "ip": "59.144.114.250", "cca3": "IND", "cn": "India", "temp": 46, "signal": 25, "battery_level": 4, "c02_level": 863, "timestamp" :1475600520 }"""),
        (12, """{"device_id": 12, "device_type": "sensor-igauge", "ip": "193.156.90.200", "cca3": "NOR", "cn": "Norway", "temp": 18, "signal": 26, "battery_level": 8, "c02_level": 1220, "timestamp" :1475600522 }"""),
        (13, """{"device_id": 13, "device_type": "sensor-ipad", "ip": "67.185.72.1", "cca3": "USA", "cn": "United States", "temp": 34, "signal": 20, "battery_level": 8, "c02_level": 1504, "timestamp" :1475600524 }"""),
        (14, """{"device_id": 14, "device_type": "sensor-inest", "ip": "68.85.85.106", "cca3": "USA", "cn": "United States", "temp": 39, "signal": 17, "battery_level": 8, "c02_level": 831, "timestamp" :1475600526 }"""),
        (15, """{"device_id": 15, "device_type": "sensor-ipad", "ip": "161.188.212.254", "cca3": "USA", "cn": "United States", "temp": 27, "signal": 26, "battery_level": 5, "c02_level": 1378, "timestamp" :1475600528 }"""),
        (16, """{"device_id": 16, "device_type": "sensor-igauge", "ip": "221.3.128.242", "cca3": "CHN", "cn": "China", "temp": 10, "signal": 24, "battery_level": 6, "c02_level": 1423, "timestamp" :1475600530 }"""),
        (17, """{"device_id": 17, "device_type": "sensor-ipad", "ip": "64.124.180.215", "cca3": "USA", "cn": "United States", "temp": 38, "signal": 17, "battery_level": 9, "c02_level": 1304, "timestamp" :1475600532 }"""),
        (18, """{"device_id": 18, "device_type": "sensor-igauge", "ip": "66.153.162.66", "cca3": "USA", "cn": "United States", "temp": 26, "signal": 10, "battery_level": 0, "c02_level": 902, "timestamp" :1475600534 }"""),
        (19, """{"device_id": 19, "device_type": "sensor-ipad", "ip": "193.200.142.254", "cca3": "AUT", "cn": "Austria", "temp": 32, "signal": 27, "battery_level": 5, "c02_level": 1282, "timestamp" :1475600536 }"""))
        .toDF("id", "device")
        .as[DeviceData]
       /**
         * get_json_object() 用法 抽取出id devicetype ip CCA3 几个字段
         * @return void
         * @author wangjx@knownsec.com
         * 抽取字段 并打扁平 然后重新组装成json
         */
          //df
//        val eventsFromJSONDF = Seq (
//          (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
//          (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""),
//          (2, """{"device_id": 2, "device_type": "sensor-ipad", "ip": "88.36.5.1", "cca3": "ITA", "cn": "Italy", "temp": 18, "signal": 25, "battery_level": 5, "c02_level": 1372, "timestamp" :1475600500 }"""),
//          (3, """{"device_id": 3, "device_type": "sensor-inest", "ip": "66.39.173.154", "cca3": "USA", "cn": "United States", "temp": 47, "signal": 12, "battery_level": 1, "c02_level": 1447, "timestamp" :1475600502 }"""),
//          (4, """{"device_id": 4, "device_type": "sensor-ipad", "ip": "203.82.41.9", "cca3": "PHL", "cn": "Philippines", "temp": 29, "signal": 11, "battery_level": 0, "c02_level": 983, "timestamp" :1475600504 }"""),
//          (5, """{"device_id": 5, "device_type": "sensor-istick", "ip": "204.116.105.67", "cca3": "USA", "cn": "United States", "temp": 50, "signal": 16, "battery_level": 8, "c02_level": 1574, "timestamp" :1475600506 }"""),
//          (6, """{"device_id": 6, "device_type": "sensor-ipad", "ip": "220.173.179.1", "cca3": "CHN", "cn": "China", "temp": 21, "signal": 18, "battery_level": 9, "c02_level": 1249, "timestamp" :1475600508 }"""),
//          (7, """{"device_id": 7, "device_type": "sensor-ipad", "ip": "118.23.68.227", "cca3": "JPN", "cn": "Japan", "temp": 27, "signal": 15, "battery_level": 0, "c02_level": 1531, "timestamp" :1475600512 }"""),
//          (8 ,""" {"device_id": 8, "device_type": "sensor-inest", "ip": "208.109.163.218", "cca3": "USA", "cn": "United States", "temp": 40, "signal": 16, "battery_level": 9, "c02_level": 1208, "timestamp" :1475600514 }"""),
//          (9,"""{"device_id": 9, "device_type": "sensor-ipad", "ip": "88.213.191.34", "cca3": "ITA", "cn": "Italy", "temp": 19, "signal": 11, "battery_level": 0, "c02_level": 1171, "timestamp" :1475600516 }"""))
//          .toDF("id", "json")
//        //必须导入functions包
//        val jsDF = eventsFromJSONDF
//          .select($"id",
//            get_json_object($"json", "$.device_type").alias("device_type"),
//            get_json_object($"json", "$.ip").alias("ip"),
//            get_json_object($"json", "$.cca3").alias("cca3"))
//        jsDF.printSchema
//        jsDF.show

        /**
         * from_json 使用schema去抽取单独列
          * 与get_json_object不同的是该方法，使用schema去抽取单独列。在dataset的api select中使用from_json()方法，
          * 也可以从一个json 字符串中按照指定的schema格式抽取出来作为DataFrame的列。
          * 还有，我们也可以将所有在json中的属性和值当做一个devices的实体。我们不仅可以使用device.arrtibute去获取特定值，也可以使用*通配符
         * @return void
         * @author wangjx@knownsec.com
         */

        val devicesDF = eventsDS.select(from_json($"device", jsonSchema) as "devices")
          .select($"devices.*")
          .filter($"devices.temp" > 10 and $"devices.signal" > 15)

        /**
         * 把所有的转换成一个json
         * @return void
         * @author wangjx@knownsec.com
         */

//        val stringJsonDF = eventsDS.select(to_json(struct($"*"))).toDF("devices")
//        stringJsonDF.show

        /**
         * 将列转化为一个JSON对象的另一种方式是使用selectExpr()功能函数。例如我们可以将device列转化为一个JSON对象。
         * @return void
         * @author wangjx@knownsec.com
         */

        val stringsDF = eventsDS.selectExpr("CAST(id AS INT)","CAST(device AS STRING)")
        stringsDF


        devicesDF.selectExpr("c02_level", "round(c02_level/temp) as ratio_c02_temperature").orderBy($"ratio_c02_temperature" desc).show

        devicesDF.createOrReplaceTempView("devicesDFT")
        spark.sql("select c02_level,round(c02_level/temp) as ratio_c02_temperature from devicesDFT order by ratio_c02_temperature desc").show


    }
}
