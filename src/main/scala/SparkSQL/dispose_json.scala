package SparkSQL

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, sql}

/**
  * Author wangjx
  * Create 2018/10/19 - 14:36
  */
case class DeviceAlert(dcId: String, deviceType: String, ip: String, deviceId: Long, temp: Long, c02_level: Long,
                       lat: Double, lon: Double)

object dispose_json {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val ssc = new sql.SparkSession
    .Builder()
      .config(conf)
      .master("local[2]")
      .appName("dispose_json")
      .getOrCreate()

    ssc.sparkContext.setLogLevel("error")
    println("--------------------------------------------------------------------")
    //导入隐式转换
    import ssc.implicits._
    val dataDS1 = Seq(
      """
        |{
        |"dc_id": "dc-101",
        |"source": {
        |    "sensor-igauge": {
        |      "id": 10,
        |      "ip": "68.28.91.22",
        |      "description": "Sensor attached to the container ceilings",
        |      "temp":35,
        |      "c02_level": 1475,
        |      "geo": {"lat":38.00, "long":97.00}
        |    },
        |    "sensor-ipad": {
        |      "id": 13,
        |      "ip": "67.185.72.1",
        |      "description": "Sensor ipad attached to carbon cylinders",
        |      "temp": 34,
        |      "c02_level": 1370,
        |      "geo": {"lat":47.41, "long":-122.00}
        |    },
        |    "sensor-inest": {
        |      "id": 8,
        |      "ip": "208.109.163.218",
        |      "description": "Sensor attached to the factory ceilings",
        |      "temp": 40,
        |      "c02_level": 1346,
        |      "geo": {"lat":33.61, "long":-111.89}
        |    },
        |    "sensor-istick": {
        |      "id": 5,
        |      "ip": "204.116.105.67",
        |      "description": "Sensor embedded in exhaust pipes in the ceilings",
        |      "temp": 40,
        |      "c02_level": 1574,
        |      "geo": {"lat":35.93, "long":-85.46}
        |    }
        |  }
        |}
      """.stripMargin).toDS()
    //定义schema
    val schema1 = new StructType()
      .add("dc_id", StringType)
      .add("source",
        MapType(StringType,
          new StructType()
            .add("description", StringType)
            .add("ip", StringType)
            .add("id", LongType)
            .add("temp", LongType)
            .add("c02_level", LongType)
            .add("geo",
              new StructType()
                .add("lat", DoubleType)
                .add("long", DoubleType)
            )
        )
      )
    val df1 = ssc.read.schema(schema1).json(dataDS1.rdd)
    df1.printSchema()
    df1.show(false)
    println("=======================================")
    val explodeDF = df1.select($"dc_id", explode($"source"))
    explodeDF.printSchema()
    explodeDF.show(10, false)
    println("=======================================")
    val notifydevicesDS = explodeDF.select($"dc_id" as "dcId",
      $"key" as "deviceType",
      'value.getItem("ip") as 'ip,
      'value.getItem("id") as 'deviceId,
      'value.getItem("c02_level") as 'c02_level,
      'value.getItem("temp") as 'temp,
      'value.getItem("geo").getItem("lat") as 'lat,
      'value.getItem("geo").getItem("long") as 'lon)
      .as[DeviceAlert]
    notifydevicesDS.printSchema()
    notifydevicesDS.show(20, false)

    ssc.stop()
  }
}