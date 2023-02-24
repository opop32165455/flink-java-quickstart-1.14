package flink.source;

import flink.utils.KafkaUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Collections;
import java.util.List;

/**
 * @author zhangxuecheng4441
 * @date 2023/2/24/024 17:35
 */
@Builder
@AllArgsConstructor
public class KafkaGenericSource<OUT> {

    /**
     * topic ...
     */
    List<String> topics;
    String topic;
    String group;
    String bootstrap;
    DeserializationSchema<OUT> schema;

    /**
     * 创建kafka source
     *
     * @return KafkaSource
     */
    public KafkaSource<OUT> createSource() {
        if (CollectionUtil.isNullOrEmpty(topics)) {
            topics = Collections.singletonList(topic);
        }

        if (StringUtils.isNullOrWhitespaceOnly(bootstrap)) {
            bootstrap = KafkaUtil.defaultBroker();
        }

        return KafkaSource.<OUT>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topics)
                .setGroupId(group)
                //偏移量 当没有提交偏移量则从最开始开始消费
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                //自定义解析消息内容
                .setValueOnlyDeserializer(schema)
                .setProperties(KafkaUtil.defaultConsumerProp())
                .build();
    }
}
