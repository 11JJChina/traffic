package utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Created by: ThinkPad 2018/4/14
  */
object RedisUtil {

  val host="192.168.30.129"
  val port=6379
  val timeout=30000

  val config=new JedisPoolConfig
  config.setMaxTotal(200)
  config.setMaxIdle(50)
  config.setMinIdle(0)
  config.setMaxWaitMillis(10000)
  config.setTestOnBorrow(true)
  config.setTestOnReturn(true)
  config.setTestOnCreate(true)

  config.setTestWhileIdle(true)
  config.setTimeBetweenEvictionRunsMillis(30000)
  config.setNumTestsPerEvictionRun(10)
  config.setMinEvictableIdleTimeMillis(60000)

  //连接池
  lazy val pool = new JedisPool(config,host,port,timeout)

  //创建程序崩溃时，回收资源线程
  lazy val hook= new Thread{
    override def run() = {
      pool.destroy()
    }
  }

  sys.addShutdownHook(hook)

}
