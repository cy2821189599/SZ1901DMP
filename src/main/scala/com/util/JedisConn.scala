package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConn {

  private val config: JedisPoolConfig = new JedisPoolConfig
  config.setMaxIdle(50)
  config.setMaxTotal(20)



  private val pool = new JedisPool(config,"localhost",6379,10000)

  def getConn(): Jedis ={
    pool.getResource
  }
}
