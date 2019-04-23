package Flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
/**
  *
  * @author wangjx 
  * 使用table sql api方式
  */
object TableSQLAPI {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val filePath = ""
    //已经拿到dataset
    val csv = env.readCsvFile[SalesLog](filePath,ignoreFirstLine = true)
    //dataset ==> table 拿到table
    val salesTable = tableEnv.fromDataSet(csv)
    //注册成一张表
    val sales = tableEnv.registerTable("sales",salesTable)
    //sql
    val resultTable = tableEnv.sqlQuery("select * from sales")

    tableEnv.toDataSet[Row](resultTable).print()
  }

  case class SalesLog(transactionId:String,customId:String,itemId:String,amountPaid:Double)
}
