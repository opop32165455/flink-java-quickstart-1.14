package flink.launch.demo;

import flink.model.FlinkBranchModel;
import flink.model.FlinkStreamModel;
import flink.source.KafkaGenericSource;
import lombok.val;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;

/**
 * @author zhangxuecheng4441
 * @date 2023/3/13/013 9:40
 */
public class ConsumerKafkaApp extends FlinkBranchModel {
    public static void main(String[] args) throws Exception {
        String topic = "test";
        String group = "test01";

        val env = FlinkStreamModel.initEnv(args);

        KafkaSource<String> source = KafkaGenericSource.<String>builder()
                .topic(topic)
                .group(group)
                .schema(new SimpleStringSchema())
                .build()
                .createSource();

        val kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Consumer Topics:" + (topic) + " By Group: " + group);

        kafkaSource.print();
        env.execute();
    }

}
