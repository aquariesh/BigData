package run

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author wangjx
   从nc服务器获取数据 实时wordcount
   sparkstreaming 方式读取nc服务器数据
 */
object SparkStreamingNCTest {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setAppName("ncTest").setMaster("local[2]")
    val ssc=new StreamingContext(conf,Seconds(10))
    //创建SocketInputDStream，接收来自 ip:port 发送来的流数据
    val lines=ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_AND_DISK_SER)
    //val lines = ssc.textFileStream("file:///home/ksuser/software/file")
    val result=lines.flatMap(_.split(" "))
      .map((_,1)).reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
