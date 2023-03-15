package flink.utils;

import cn.hutool.setting.dialect.Props;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author zhangxuecheng4441
 * @date 2022/2/22/022 17:03
 */
public class KafkaUtil {
    /**
     * 默认消费者配置
     *
     * @return Properties
     */
    public static Properties defaultConsumerProp() {
        return Props.getProp("kafka-consumer.properties");
    }

    /**
     * 默认生产者配置
     *
     * @return Properties
     */
    public static Properties defaultProducerProp() {
        Properties properties = new Properties();
        //flink KafkaSinkBuilder 取消了此配置 使用了ByteArraySerializer
        //properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, defaultBroker());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //最大提交消息
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384000);
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 30 * 1000);
        return properties;
        // return Props.getProp("kafka-producer.properties"); 使用配置文件读取会发生错误
    }

    public static String defaultBroker() {
        Props prop = Props.getProp("kafka-consumer.properties");
        return prop.getStr("bootstrap.servers");
    }
}
