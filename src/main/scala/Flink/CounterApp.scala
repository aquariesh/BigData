package Flink

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  *
  * @author wangjx 
  * 计数器
  */
object CounterApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val data = env.fromElements("hadoop","spark","flink","pyspark","storm")
    //RichMap 不能符合要求 因为并行度一多 就不能实现计数功能
//    data.map(new RichMapFunction[String,Long]() {
//      var counter = 0l
//      override def map(value: String): Long = {
//        counter = counter + 1
//        println("counter" + counter)
//        counter
//      }
//    }).setParallelism(5).print()
    val info = data.map(new RichMapFunction[String,String]() {

      //step1 定义计数器
      val counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        //step2 注册计数器
        getRuntimeContext.addAccumulator("ele-counts-scala",counter)
      }
      override def map(value: String): String = {
        counter.add(1)
        value
      }
    }).print()
    //step3 获取计数器
    val jobResult = env.execute("CounterApp")
    val num = jobResult.getAccumulatorResult[Long]("ele-counts-scala")
    println("num+"+num)
  }
}
