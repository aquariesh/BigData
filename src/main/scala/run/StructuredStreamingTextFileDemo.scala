package run

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 *
 * @author wangjx
 * 从固定文件读取
 */
object StructuredStreamingTextFileDemo {

  def main(args: Array[String]): Unit = {

    //    创建sparksession
    val spark = SparkSession
      .builder
      .master("local")
      .appName("StructuredStreamingDemo")
      .getOrCreate()

    import spark.implicits._

    //  定义schema
    val userSchema = new StructType().add("name", "string").add("age", "integer")

    // 读取数据
    val dataFrameStream = spark
      .readStream
      .schema(userSchema)
      .format("json")
      //只能是监控目录，当新的目录进来时，再进行计算
      .load("/home/ksuser/software/json")


    //dataFrameStream.isStreaming
    //注册成一张表
    dataFrameStream.createOrReplaceTempView("person")

    // 对表进行agg 如果没有agg的话，直接报错
    val aggDataFrame = spark.sql(
      """
        |select
        |name,
        |age,
        |count(1) as cnt
        |from
        |person
        |group by
        |name,age
      """.stripMargin
    )

    //在控制台监控
    val query = aggDataFrame.writeStream
      //complete,append,update。目前只支持前面两种
      .outputMode("complete")
      //console,parquet,memory,foreach 四种
      .format("console")
      //这里就是设置定时器了
      //.trigger(ProcessingTime(100))
      .start()

    query.awaitTermination()

  }
}


