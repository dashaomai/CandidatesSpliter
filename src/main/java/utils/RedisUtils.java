package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Redis 管理器类，内含一个 Jedis 连接池
 * <p>
 * Created by dasha on 2017/6/9.
 */
public class RedisUtils {
  private static final Logger log = LoggerFactory.getLogger("RedisUtils");

  private static JedisPool pool;

  public static void Init(final String host, final int port) {
    if (null == pool) {
      pool = new JedisPool(host, port);
    } else {
      log.warn("Jedis pool was already initialized!");
    }
  }

  /**
   * 从连接池中获取一个 Jedis 客户端
   *
   * @return
   */
  private static Jedis GetClient() {
    log.debug("获取 Redis 连接");

    try {
      return pool.getResource();
    } catch (Exception ex) {
      log.error("无法获取 Redis 客户端：{}", ex.getLocalizedMessage());

      return null;
    }
  }

  /**
   * 向连接池中返还一个 Jedis 客户端
   *
   * @param client
   */
  private static void ReleaseClient(final Jedis client) {
    if (null != client) {
      log.debug("归还 Redis 连接");
      client.close();
    }
  }

  /**
   * 执行 Redis 命令的模板
   *
   * @param execution
   * @param <T>
   * @return
   */
  public static <T> T Execute(final IRedisExecution<T> execution) {
    Jedis client = GetClient();

    if (null != client) {
      try {
        final T result = execution.execute(client);

        ReleaseClient(client);
        client = null;

        return result;
      } catch (Exception ex) {
        log.error("执行 Redis 操作时出错：{}", ex.getLocalizedMessage());

        return null;
      } finally {
        if (null != client) {
          ReleaseClient(client);
        }
      }
    } else {
      log.error("无法从 Jedis 连接池中获得 Redis 连接");
      return null;
    }
  }
}
