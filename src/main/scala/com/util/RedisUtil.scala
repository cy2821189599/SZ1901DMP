package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
  val config = new JedisPoolConfig()
  // 设置最大连接数
  config.setMaxTotal(20)
  // 最大空闲连接数
  config.setMaxIdle(10)

  // 创建pool对象
  private val pool = new JedisPool(config,"127.0.0.1",6379,10000)

  //创建连接
  def getConnection(): Jedis ={
    pool.getResource
  }
}
