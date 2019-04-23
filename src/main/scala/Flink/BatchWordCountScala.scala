package Flink

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer
import org.apache.flink.api.scala._
/**
  *
  * @author wangjx@knownsec.com
  *         @2019/4/10 14:43
  *         使用scala开发flink的批处理应用程序
  */
object BatchWordCountScala {
  def main(args: Array[String]): Unit = {
    val input = "file:///Users/wangjx/data/log.txt"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(input)
    //需要导入隐式转换
    //text.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).groupBy(0).sum(1).print()
    //firstFunction(env)
    //flatMapFunction(env)
    joinFunction(env)
  }

  def firstFunction(env:ExecutionEnvironment): Unit ={
    val info = ListBuffer[(Int,String)]()
    info.append((1,"hadoop"))
    info.append((1,"spark"))
    info.append((1,"flink"))
    info.append((2,"java"))
    info.append((2,"spring boot"))
    info.append((3,"linux"))
    info.append((4,"wangjx"))

    val data = env.fromCollection(info)
    data.first(3).print()
    data.groupBy(0).sortGroup(1,Order.DESCENDING).first(2).print()
  }

  def flatMapFunction(env:ExecutionEnvironment): Unit ={
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")

    val data = env.fromCollection(info)
    data.flatMap(_.split(",")).map((_,1)).groupBy(0).sum(1).print()
  }

  def joinFunction(env:ExecutionEnvironment): Unit ={
    val info1 = ListBuffer[(Int,String)]()
    info1.append((1,"猪蹄膀"))
    info1.append((2,"猪大肠"))
    info1.append((3,"猪头肉"))
    info1.append((4,"猪肥膘"))

    val info2 = ListBuffer[(Int,String)]()
    info2.append((1,"北京"))
    info2.append((2,"上海"))
    info2.append((3,"成都"))
    info2.append((5,"杭州"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.join(data2).where(0).equalTo(0).apply((first,second)=>{
      (first._1,first._2,second._2)
    }).print()
  }
}
