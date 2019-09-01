package main

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.{JSON, TypeReference}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import utils.{PropertyUtil, RedisUtil}


/**
  * Created by: ThinkPad 2018/4/14
  */
object SparkConsumer {

  def main(args: Array[String]): Unit = {

    //初始化spark
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TrafficStream")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./ssc/checkpoint")

    //配置kafka参数
    val kafkaParams = Map("metadata.broker.list" -> PropertyUtil.getProperty("metadata"))

    //想要消费的数据在哪个主题中
    val topics = Set(PropertyUtil.getProperty("kafka.topics"))

    //读取kafka中的value数据
    val kafkaLineDStream = KafkaUtils.createDirectStream[
      String,
      String,
      StringDecoder,
      StringDecoder](ssc, kafkaParams, topics)
      .map(_._2)

    //解析我们读取到的数据
    val event = kafkaLineDStream.map(line => {
      //json解析
      val lineJavaMap = JSON.parseObject(line, new TypeReference[java.util.Map[String, String]]() {})

      //将javaMap转为scalaMap
      import scala.collection.JavaConversions._
      val lineScalaMap: collection.mutable.Map[String, String] = mapAsScalaConverter(lineJavaMap)
        .asScala
      println(lineScalaMap)
      lineScalaMap
    })

    //将数据简单聚合
    val sumOfSpeedAndCount = event
      .map(e => (e.get("monitor_id").get, e.get("speed").get)) //("0001","50")
      .mapValues(v => (v.toInt, 1)) //("0001",(50,1))
      .reduceByKeyAndWindow((t1: (Int, Int), t2: (Int, Int)) =>
      (t1._1 + t2._1, t1._2 + t2._2), Seconds(60), Seconds(60)) //("0001",(500,10))

    //将上边处理好的数据存到redis中
    val dbIndex = 1
    sumOfSpeedAndCount.foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecord => {
        partitionRecord
          .filter((tuple: (String, (Int, Int))) => tuple._2._2 > 0)
          .foreach(pair => {
            val jedis = RedisUtil.pool.getResource
            val monitorId = pair._1
            val sumOfSpeed = pair._2._1
            val sumOfCarCount = pair._2._2

            val currentTime = Calendar.getInstance().getTime
            val dateSDF = new SimpleDateFormat("yyyyMMdd")
            val hmSDF = new SimpleDateFormat("HHmm")

            val hourMinuteTime = hmSDF.format(curreentTime)
            val date = dateSDF.format(currentTime)

            jedis.select(dbIndex)
            jedis.hset(date + "_" + monitorId, hourMinuteTime, sumOfSpeed + "_" + sumOfCarCount)

            RedisUtil.pool.returnResource(jedis)

          })
      })
    })

    ssc.start
    ssc.awaitTermination

  }


}
