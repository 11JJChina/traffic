package main

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import utils.RedisUtil

import scala.collection.mutable.ArrayBuffer
import scala.collection.script.Index

/**
  * Created by: ThinkPad 2018/4/15
  * 数据建模
  */
object Train {
  def main(args: Array[String]): Unit = {
    //将本次评估结果保存到下面这个文件
    val writer = new PrintWriter(new File("model_training.txt"))

    //配置spark
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TrafficTrain")
    val sc = new SparkContext(sparkConf)

    //redis相关
    val dbIndex = 1
    val jedis = RedisUtil.pool.getResource
    jedis.select(dbIndex)

    //设置想要对哪个监测点进行数据建模
    val monitorIDs = List("0005", "0015")
    //对上面两个监测点进行建模，但是这两个监测点可能需要其他监测点的数据信息
    val monitorRelations = Map[String, Array[String]](
      "0005" -> Array("0003", "0004", "0005", "0006", "0007"),
      "0015" -> Array("0013", "0014", "0015", "0016", "0017")
    )

    //遍历上面所有监测点，进行建模
    monitorRelations.map(monitorId => {
      val monitorRelationList = monitorRelations.get(monitorId).get
      //处理时间
      val currentDate = Calendar.getInstance.getTime
      //设置时间格式化
      val hourMinuteSDF = new SimpleDateFormat("HHmm")
      val dateSDF = new SimpleDateFormat("yyyyMMdd")
      val dateOfString = dateSDF.format(currentDate)

      val relationInfo =  monitorRelationList.map(monitorId => (monitorId,jedis.hgetAll(dateOfString + "_" + monitorId)))

      //使用n个小时内的数据进行建模
      val hours = 1

      val dataTrain = ArrayBuffer[LabeledPoint]()
      val dataX = ArrayBuffer[Double]()
      val dataY = ArrayBuffer[Double]()

      //将时间拉回到一个小时之前
      for(i <- Range(60 * hours,2,-1)){
        dataX.clear
        dataY.clear
        for(Index <- 0 to 2){
          val oneMoment = currentDate.getTime - 60 * i * 1000 + 60 * index * 1000
          val oneHM = hourMinuteSDF.format(new Date(oneMoment))

          for((k,v) <- relationInfo){
            if(k == monitorId && index == 2){
              //第四分钟数据
              val nextMoment = oneMoment + 60 * 1000
              val nextHM = hourMinuteSDF.format(new Date(nextMoment))

              if(v.containsKey(nextHM)){
                val speedAndCarCount = v.get(nextHM).split("_")
                val valueY = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat
                dataY += valueY
              }

              if(v.containsKey(oneHM)){
                val speedAndCarCount = v.get(oneHM).split("_")
                val valueX = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat
                dataX += valueX

              }else{
                dataX += -1.0F
              }

            }
          }

          //训练模型
          if(dataY.toArray.length == 1){
            val label = dataY.toArray.head
            val record = LabeledPoint(if(label.toInt / 10 < 10) label.toInt / 10 else 10 ,Vectors.dense(dataX.toArray))
            dataTrain +=record
          }

          dataTrain.foreach(println(_))

          val rddData = sc.parallelize(dataTrain)

          //切分数据集
          val random = rddData.randomSplit(Array(0.6,0.4),10L)

          val trainingData = randomSplits(0)
          val testData = randomSplits(1)

          if(!rddData.isEmpty()){
            val model = new LogisticRegressionWithLBFGS().setNumClasses(11).run(trainingData)
            val predictionAndLabels = testData.map{
              case LabeledPoint(label,features) => val prediction = model.predict(features);(prediction,label)
            }

            //得到当前监测点model的评估值
            val metric = new MulticlassMetrics(predictionAndLabels)

            val accuracy = metric.accuracy
            println("评估值：" + accuracy)

            writer.write(accuracy.toString + "\r\n")

            if(accuracy > 0.9){
              val hdfsPath = "hdfs://hadoop101:9000/traffic/model" + monitorId + "_" + new SimpleDateFormat("yyyyMMddHHmmss").format(currentDate.getTime)
              model.save(sc,hdfsPath)
              jedis.hset("model",monitorId,hdfsPath)
            }

          }

        }
      }
      null

    })
    RedisUtil.pool.getResource(jedis)
    writer.flush
    writer.close

  }

}
