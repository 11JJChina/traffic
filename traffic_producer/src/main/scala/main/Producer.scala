package main

import java.text.DecimalFormat
import java.util
import java.util.Calendar

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import utils.PropertyUtil

import scala.util.Random

/**
  * Created by: ThinkPad 2018/4/13
  */
object Producer {
  def main(args: Array[String]): Unit = {
    //读取kafka配置信息
    val props = PropertyUtil.properties

    //创建kafka生产者对象
    val producer = new KafkaProducer[String,String](props)

    //模拟生产实时数据，每五分钟切换一次车速
    var startTime = Calendar.getInstance().getTimeInMillis / 1000

    val trafficCycle = 300

    //不停地模拟数据
    while(true){
      //模拟监测点
      val randomMonitorId = new DecimalFormat("0000").format(Random.nextInt(20) + 1)
      //定义一个车速
      var randomSpeed = "";

      //模拟（堵车）切换周期：5分钟
      val currnetTime = Calendar.getInstance().getTimeInMillis / 1000

      if(currnetTime - startTime > 300){
        randomSpeed = new DecimalFormat("000").format(Random.nextInt(16))

        if(currnetTime - startTime > trafficCycle * 2){
          startTime = currnetTime
        }

      }else{
        randomSpeed = new DecimalFormat("000").format(Random.nextInt(31) + 30)

      }

//      println(randomMonitorId + "," + randomSpeed)
      val jsonMap=new util.HashMap[String,String]()
      jsonMap.put("monitor_id",randomMonitorId)
      jsonMap.put("speed",randomSpeed)

      //序列化json
      val event= JSON.toJSON(jsonMap)

      producer.send(new ProducerRecord[String,String]
      (props.getProperty("kafka.topics"),
        event.toString))

      Thread.sleep(200)


    }


  }

}
