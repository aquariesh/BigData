package dao

import SparkStreaming.FlumeKafkaTest
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import utils.HbaseUtils

import scala.collection.mutable.ListBuffer

/**
  * 实战课程点击数据访问层
  */
object CourseClickCountDao {
    val tableName = "imooc_course_clickcount"  //表明
    val cf = "info" //列族
    val qualifer = "click_count" //列名 动态添加就可以 创建时不用指定
    /**
    * 保存数据到hbase
    * @param list   CourseClickCount集合
    */
    def save(list:ListBuffer[FlumeKafkaTest.CourseClickCount]): Unit ={
       val table = HbaseUtils.getInstance().getTable(tableName)

        for (ele <- list){
                table.incrementColumnValue(Bytes.toBytes(ele.day_course),
                Bytes.toBytes(cf),
                Bytes.toBytes(qualifer),
                ele.click_count)
        }
    }

    /**
    * 根据rowkey查询值
    * @param day_course
    * @return
    */
    def count(day_course:String):Long={
        val table = HbaseUtils.getInstance().getTable(tableName)

        val get = new Get(Bytes.toBytes(day_course))
        val value = table.get(get).getValue(cf.getBytes,qualifer.getBytes)

        if(value == null){
            0l
        }
        else {
            Bytes.toLong(value)
        }
    }
}
