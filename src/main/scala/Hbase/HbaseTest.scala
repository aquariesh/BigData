package Hbase

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 1.0新api
  *
  * @author wangjx
  */
object HbaseTest {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("hbase").setMaster("local[2]")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    //建议使用单例模式创建连接
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "master")
    val conn = ConnectionFactory.createConnection(conf)

    val admin = conn.getAdmin
    //创建user表
    val userTable = TableName.valueOf("user")

    val tableDescr = new HTableDescriptor(userTable)

    tableDescr.addFamily(new HColumnDescriptor("basic".getBytes))

    println("Creating table `user`. ")

    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    }

    admin.createTable(tableDescr)
    println("Done!")
    //获取 user 表
    val table = conn.getTable(userTable)
    //准备插入一条 key 为 id001 的数据
    val put = new Put("id001".getBytes)
    //为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
    put.addColumn("basic".getBytes, "name".getBytes, "wangjx".getBytes)
    table.put(put)
    //查询某条数据
    val get = new Get("id001".getBytes)
    val result = table.get(get)
    //获取到对应列簇对应列的数据
    val value = Bytes.toString(result.getValue("basic".getBytes, "name".getBytes))
    println("GET id001 :" + value)
    //扫描数据
    val scan = new Scan()
    scan.addColumn("basci".getBytes, "name".getBytes)
    val scanner = table.getScanner(scan)
//    for (r <- scanner) {
//      println("Found row " + r)
//    }
    //删除某条数据,操作方式与 Put 类似
    val delete = new Delete("id001".getBytes)
    delete.addColumn("basic".getBytes, "name".getBytes)
    table.delete(delete)

    if (table != null) table.close()

    conn.close()

    /**
      * 首先要向 HBase 写入数据，我们需要用到PairRDDFunctions.saveAsHadoopDataset。
      * 因为 HBase 不是一个文件系统，所以saveAsHadoopFile方法没用。
      * def saveAsHadoopDataset(conf: JobConf): Unit
      * Output the RDD to any Hadoop-supported storage system,
      * using a Hadoop JobConf object for that storage system
      * 这个方法需要一个 JobConf 作为参数，类似于一个配置项，主要需要指定输出的格式和输出的表名。
      *
      * @return void
      * @author wangjx@knownsec.com
      */


    /**
     *  第一步，创建一个JobConf
     */
    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "user")

    /**
     *  第二步，RDD 到表模式的映射
      * 在 HBase 中的表 schema 一般是这样的：
      * row     cf:col_1    cf:col_2
      * 而在spark中，我们操作的是rdd元祖，比如(1,"lilei",14), (2,"hanmei",18)
      * 我们需要将 RDD[(uid:Int, name:String, age:Int)] 转换成 RDD[(ImmutableBytesWritable, Put)]。
      * 所以，我们定义一个 convert 函数做这个转换工作
     */
      def convert(triple:(Int,String,Int))={
        val put = new Put(Bytes.toBytes(triple._1))
        put.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("name"),Bytes.toBytes(triple._2))
        put.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("age"),Bytes.toBytes(triple._3))
        (new ImmutableBytesWritable,put)
      }
      /**
       * 第三步 读取rdd并转换
       */

      val rawData = List((1,"lilei",14), (2,"hanmei",18), (3,"someone",38))
      val localData = spark.sparkContext.parallelize(rawData).map(convert)
      /**
       * 第四步 使用saveAsHadoopDataset方法写入hbase
       */
      localData.saveAsHadoopDataset(jobConf)


      /**
       *  读取hbase Spark读取HBase，
        *  我们主要使用SparkContext 提供的newAPIHadoopRDD API将表的内容以 RDDs 的形式加载到 Spark 中。
        *
       */
       val readConf = HBaseConfiguration.create()
       readConf.set("hbase.zookeeper.property.clientPort", "2181")
       readConf.set("hbase.zookeeper.quorum", "master")

       //设置查询的表名

       readConf.set(TableInputFormat.INPUT_TABLE, "user")
       val usersRDD = spark.sparkContext.newAPIHadoopRDD(readConf,
         classOf[TableInputFormat],
         classOf[ImmutableBytesWritable],
         classOf[Result])

       val count = usersRDD.count()
       println("Users RDD Count:" + count)
       usersRDD.cache()
       usersRDD.foreach(rdd=>{
         val result = rdd._2
         val key = Bytes.toInt(result.getRow)
         val name = Bytes.toString(result.getValue("basic".getBytes,"name".getBytes))
         val age = Bytes.toInt(result.getValue("basic".getBytes,"age".getBytes))
         println("Row key:"+key+" Name:"+name+" Age:"+age)
       })
    //遍历输出
//      usersRDD.foreach{ case (_,result) =>
//        val key = Bytes.toInt(result.getRow)
//        val name = Bytes.toString(result.getValue("basic".getBytes,"name".getBytes))
//        val age = Bytes.toInt(result.getValue("basic".getBytes,"age".getBytes))
//        println("Row key:"+key+" Name:"+name+" Age:"+age)
//    }

  }
}
