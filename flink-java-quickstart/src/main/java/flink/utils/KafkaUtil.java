package flink.utils;

import cn.hutool.setting.dialect.Props;

import java.util.Properties;

/**
 * @author zhangxuecheng4441
 * @date 2023/2/22/022 17:03
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
        return Props.getProp("kafka-producer.properties");
    }

    public static String defaultBroker(){
        Props prop = Props.getProp("kafka-consumer.properties");
        return prop.getStr("bootstrap.servers");
    }
}
