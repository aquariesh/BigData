import org.apache.spark.sql.SparkSession

/**
  * @author wangjx
  */
object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hive地址")
      .getOrCreate();
    val lines = spark.sparkContext.textFile("file:///Users/wangjx/data/test.log")
    val line = lines.map(m => {
      val start = m.indexOf("{")
      val end = m.indexOf("}")
      println(start)
      println(end)
      val log = m.substring(start, end + 1)
      println(log)
      log
    })
    line.saveAsTextFile("file:///Users/wangjx/data/log.log")
    //val peopleDF=spark.read.json("/people.json")
    //peopleDF.write.save("")
    //peopleDF.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")
  }
}
