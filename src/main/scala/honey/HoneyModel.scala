package honey

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.SparkSession

object HoneyModel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("HoneyModel")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    /**
      * load data
      */
    import spark.implicits._
    val data = spark.read.format("csv")
      .load("/Users/wangjx/data/iris.data")
    //data.show(false)
    val random = new util.Random()
    val tempData = data.map(row => {
      (row.getString(0).toDouble,
        row.getString(1).toDouble,
        row.getString(2).toDouble,
        row.getString(3).toDouble,
        row.getString(4),
        random.nextDouble())
    }
    ).toDF("_c0", "_c1", "_c2", "_c3", "label" , "random" ).sort("random")
    //tempData.show(false)
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedlabel")
      .fit(tempData)
    val resultData = labelIndexer.transform(tempData)
    //result.show(false)
    /**
      * 构建封装器 封装特征列
      */
    val assembler = new VectorAssembler()
      .setInputCols(Array("_c0", "_c1", "_c2", "_c3"))
      .setOutputCol("features")
    val finalData = assembler.transform(resultData)
    //finalData.show(false)
    val Array(trainData, testData) = finalData.randomSplit(Array(0.7, 0.3))

    val tree = new DecisionTreeClassifier()
      .setLabelCol("indexedlabel")
      .setFeaturesCol("features")
    val model = tree.fit(trainData)

    //model.save("/Users/wangjx/data/model")
    model.save("hdfs://localhost:9000/model")
    val testResult = model.transform(testData)
    //testResult.write.save("/Users/wangjx/data/modelResult")
    //testResult.show(false)
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)
    val result = labelConverter.transform(testResult)
    result.show(false)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedlabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(testResult)
    println(s"""accuracy is $accuracy""")

    spark.stop()
  }
}
