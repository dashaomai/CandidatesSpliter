package utils;

import redis.clients.jedis.JedisCommands;

/**
 * Redis 执行接口，用于配合 RedisUtils.Execute 方法，编写 JAVA8 对应的 lamba 表达式
 *
 * Created by Bob Jiang on 2017/6/11.
 */
public interface IRedisExecution<T> {
    T execute(final JedisCommands client);
}
