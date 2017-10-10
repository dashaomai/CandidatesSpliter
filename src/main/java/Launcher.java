import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.RedisUtils;

import java.io.*;
import java.util.Iterator;
import java.util.Set;

public class Launcher {
    private static final Logger log = LoggerFactory.getLogger("Launcher");
    private static final String key = "candidates";
    private static final String path = "Candidates.csv";

    public static void main(String[] args) {
        RedisUtils.Init("localhost", 6379);

//        popCandidate();
//        restoreCandidates();
//        removeCandidate();
    }

    private static void popCandidate() {
        // 从 Redis 中取出一半关键词
        final Boolean result = RedisUtils.Execute(client -> {
            if (client.exists(key)) {
                final Long count = client.scard(key);

                if (count > 10) {
                    final Long amount = count / 2;
                    log.info("当前关键词数量：{} ，可以分裂：{} 个", count, amount);

                    final Set<String> candidates = client.spop(key, amount);
                    final Iterator<String> iterator = candidates.iterator();
                    final StringBuilder builder = new StringBuilder();
                    while (iterator.hasNext()) {
                        final String candidate = iterator.next();
                        builder.append(candidate);
                        builder.append(",");
                    }

                    final String content = builder.toString().substring(0, builder.length() - 1);
                    final File file = new File(path);

                    try {
                        if (!file.exists())
                            if (!file.createNewFile()) {
                                log.error("创建文件 {} 失败", path);
                                return false;
                            }

                        final OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(path));
                        writer.write(content);
                        writer.flush();
                        writer.close();
                    } catch (IOException e) {
                        log.error("创建文件 {} 失败：{}", path, e.getMessage());
                    }

                } else {
                    log.info("当前关键词数量：{} 小于 10 个，无需分裂", count);
                }

                return true;
            } else {
                return false;
            }
        });

        log.info("Redis 操作结果：{}", result);
    }

    private static void restoreCandidates() {
        try {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            final String content = reader.readLine();
            final String[] params = content.split(",");

            final Boolean result = RedisUtils.Execute(client -> client.sadd(key, params) == params.length);

            log.info("Redis Restore 操作结果：{}", result);
        } catch (FileNotFoundException e) {
            log.error("读取文件 {} 未找到：{}", path, e.getMessage());
        } catch (IOException e) {
            log.error("读取文件 {} IO 错误：{}", path, e.getMessage());
        }
    }

    private static void removeCandidate() {
        try {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            final String content = reader.readLine();
            final String[] params = content.split(",");

            final Boolean result = RedisUtils.Execute(client -> client.srem(key, params) == params.length);

            log.info("Redis Remove 操作结果：{}", result);
        } catch (FileNotFoundException e) {
            log.error("读取文件 {} 未找到：{}", path, e.getMessage());
        } catch (IOException e) {
            log.error("读取文件 {} IO 错误：{}", path, e.getMessage());
        }
    }
}
