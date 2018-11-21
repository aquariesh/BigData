package run

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
 *
 * @author wangjx
 * StructuredStreaming kafka自动维护offset demo  结合sql并且控制台打印
  * 数据内容：{"a":"1","b":"2","c":"2018-01-08"}
 */
object StructuredStreamingAutoKafkaOffset {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("StructuredStreamingAutoKafkaOffset")
      .getOrCreate()

    val topic = "kafka6"
    import spark.implicits._


    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","x:9092")
      .option("subscribe",topic)
      .option("startingOffsets", "earliest")
      .load()

    //打印kafka schema
    df.printSchema()
    //根据数据类型创建schema
    val schema = StructType(mutable.Seq(
      StructField("a",DataTypes.StringType),
      StructField("b",DataTypes.StringType),
      StructField("c",DataTypes.StringType)
    ))

    val jsonDF = df.selectExpr("CAST(key as string)","CAST (value as string)")
      //解析json格式  并把value重命名为data
      .select(from_json($"value",schema=schema).as("data"))

    jsonDF.select("data.*").createOrReplaceTempView("people")

    val sql = "select * from people"

    val frame: DataFrame = spark.sql(sql)

    frame.printSchema()
    //在控制台监控
    val query = frame.writeStream
      //complete,append,update 目前只支持前面两种
      .outputMode("append")
      //console,parquet,memory,foreach 四种
      .format("console")
      .start()

    query.awaitTermination()
  }
}
