package honey

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.feature.{IndexToString, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark

object HoneyOffLine {

  case class Flower (column1:Double,column2:Double,column3:Double,
                     column4:Double,prediction:Double,flowerType:String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HoneyOffLine").setMaster("local")
        conf.set("es.index.auto.create", "true")
        conf.set("es.nodes", "127.0.0.1")
        conf.set("es.port", "9200")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val iris = spark.read.format("csv").load("/Users/wangjx/data/iristest.data")
      .map(row => (
        row.getString(0).toDouble,
        row.getString(1).toDouble,
        row.getString(2).toDouble,
        row.getString(3).toDouble
      ))
      .toDF("c1", "c2", "c3", "c4")
    //iris.show(false)
    val assembler = new VectorAssembler()
      .setInputCols(Array("c1", "c2", "c3", "c4"))
      .setOutputCol("features")
    val tempResult = assembler.transform(iris)
    //result.show(false)
    val model = DecisionTreeClassificationModel.load("hdfs://localhost:9000/model")
    val result = model.transform(tempResult)
    val finalresult = result.select("c1", "c2", "c3", "c4", "prediction")
      .map(row => {
        var flowerType = ""
        row.getDouble(4) match {
          case 0.0 => flowerType = "Iris-setosa"
          case 1.0 => flowerType = "Iris-versicolor"
          case 2.0 => flowerType = "Iris-virginica"
        }
        (row.getDouble(0),
          row.getDouble(1),
          row.getDouble(2),
          row.getDouble(3),
          row.getDouble(4),
          flowerType)
      }).toDF("c1","c2","c3","c4","prediction","flowerType")
    val saveResult = finalresult.rdd.map(rdd => Flower(rdd.getDouble(0), rdd.getDouble(1), rdd.getDouble(2), rdd.getDouble(3), rdd.getDouble(4), rdd.getString(5)))
    EsSpark.saveToEs(saveResult,"/flower/doc")
    //finalresult.show(false)
  }
}
