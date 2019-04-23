package Flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * @author wangjx 
  * 使用scala开发flink的实时处理应用程序
  */
object StreamingWordCountScalaCaseClass {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost",9999)

    import org.apache.flink.api.scala._

    text.flatMap(_.split(" "))
      .map(x=>WC(x,1))
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")
      .print()
  }

  case class WC(val word :String,val count:Integer)
}
