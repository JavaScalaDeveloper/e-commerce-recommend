package com.huat.huangjiahao.online


object ConnectRedis {
  def main(args: Array[String]): Unit = {

  import redis.clients.jedis.Jedis

  val jedis = new Jedis("linux", 6379)
  System.out.println(jedis.ping)
  }
}
