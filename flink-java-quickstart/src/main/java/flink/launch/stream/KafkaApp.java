package flink.launch.stream;

import flink.model.FlinkStreamModel;
import flink.utils.KafkaUtil;
import lombok.val;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * consumer kafka and producer kafka demo
 * todo IDEA run main方法时 need [add IDEA provided选项 ] AND [add -local local to program argument]
 *
 * @author zhangxuecheng4441
 * @date 2022/3/13/013 9:40
 */
public class KafkaApp extends FlinkStreamModel {
    public static void main(String[] args) throws Exception {
        String inTopic = "test";
        String outTopic = "test1";
        String group = "test01";

        initEnv(args);

        //consume kafka: user the kafka kafka-console-producer.sh can input message
        KafkaSource<String> source =  KafkaSource.<String>builder()
                .setBootstrapServers(KafkaUtil.defaultBroker())
                .setTopics(inTopic)
                .setGroupId(group)
                //偏移量 当没有提交偏移量则从最开始开始消费
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                //自定义解析消息内容
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(KafkaUtil.defaultConsumerProp())
                .build();

        val kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Consumer Topics:" + (inTopic) + " By Group: " + group)
                .setParallelism(2);

        //can find kafka-console-producer.sh message print out
        kafkaSource.print();

        //produce kafka: user the kafka-console-consumer.sh can find the message
        val outSink = KafkaSink.<String>builder()
                .setBootstrapServers(KafkaUtil.defaultBroker())
                .setKafkaProducerConfig(KafkaUtil.defaultProducerProp())
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .setKeySerializationSchema(new SimpleStringSchema())
                                .build()
                )
                //语义保证，保证至少一次
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        kafkaSource.sinkTo(outSink).setParallelism(4).name("Produce Topic " + outTopic);

        //todo debug 增加参数 -local local 可以IDEA测试开启 http://localhost:8081/ 研发环境
        env.execute("KafkaDemoApp");
    }

}
