package utils

import java.util.Properties

/**
  * Created by: ThinkPad 2018/4/12
  */
object PropertyUtil {
  val properties = new Properties

  try {
    val inputStream = ClassLoader.getSystemResourceAsStream("kafka.properties")

    properties.load(inputStream)
  } catch {
    case e:Exception => e.getStackTrace
  } finally {

  }

  def getProperty(key:String):String = properties.getProperty(key)

}
