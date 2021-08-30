package com.huat.huangjiahao.online;

import redis.clients.jedis.Jedis;


public class RedisConn {

    public static void main(String[] args) {
        //连接本地的 Redis 服务
        Jedis jedis = new Jedis("192.168.1.100", 6379);
        //查看服务是否运行，打出pong表示OK
        System.out.println("connection is OK==========>: " + jedis.ping());
    }
}

