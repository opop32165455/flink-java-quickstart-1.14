package flink.sink;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author zhangxuecheng4441
 * @date 2022/10/12/012 21:54
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EsSink<T> {
    private ElasticsearchSink.Builder<T> esSinkBuilder;
    private Map<String, String> config;
    private ElasticsearchSinkFunction<T> esSinkFunc;

    public ElasticsearchSink<T> getSink() {
        List<HttpHost> httpHosts = new ArrayList<>();
        String uriStrings = config.get("es.hosts");

        String[] split = uriStrings.split(",");
        for (String str : split) {
            httpHosts.add(HttpHost.create(str));
        }

        esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts, esSinkFunc);
        // 设置批量写数据的缓冲区大小
        esSinkBuilder.setBulkFlushMaxActions(50);
        //the bulk flush interval, in milliseconds.批量写入的时间间隔，配置后则会按照该时间间隔严格执行，无视上面的两个批量写入配置
        //用来表示是否开启重试机制
        esSinkBuilder.setBulkFlushBackoff(true);
        //重试策略，又可以分为以下两种类型
        //a、指数型，表示多次重试之间的时间间隔按照指数方式进行增长。eg:2 -> 4 -> 8 ...
        //b、常数型，表示多次重试之间的时间间隔为固定常数。eg:2 -> 2 -> 2 ...CONSTANT
        esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL);
        //进行重试的时间间隔。对于指数型则表示起始的基数
        esSinkBuilder.setBulkFlushBackoffDelay(2000L);
        //失败重试的次数
        esSinkBuilder.setBulkFlushBackoffRetries(5);
        return esSinkBuilder.build();
    }
}
