package Flink

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  *
  * @author wangjx 
  * 分布式缓存 就是spark里的广播变量
  */
object DIstributedCacheApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val filePath = "file:///User/wangjx"
    //step1 注册一个本地或者hdfs文件
    env.registerCachedFile(filePath,"pk-scala-dc")

    import org.apache.flink.api.scala._

    val data = env.fromElements("hadoop","spark","flink","pyspark","storm")

    data.map(new RichMapFunction[String,String] {
      //step2 在open方法中获取到分布式缓存到内容即可
      override def open(parameters: Configuration): Unit = {
        //获取到分布式缓存文件
        val dcFile = getRuntimeContext.getDistributedCache.getFile("pk-scala-dc")
        //读取分布式缓存文件
        val lines = FileUtils.readLines(dcFile)
        /**
         *
         * @return void
         * @author wangjx@knownsec.com
          *         此时会出现一个异常 java集合和scala集合不兼容的问题
         */
        import scala.collection.JavaConverters._
        for (ele <- lines.asScala){
          println(ele)
        }
      }
      override def map(value: String): String = {
        value
      }
    })
  }
}
