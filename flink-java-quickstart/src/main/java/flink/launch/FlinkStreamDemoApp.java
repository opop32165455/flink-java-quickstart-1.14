package flink.launch;

import flink.model.FlinkStreamModel;
import flink.source.KafkaGenericSource;
import lombok.val;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;

/**
 * 重点人员写入Es
 * com.surfilter.kf.etl.launch.PersonnelKafka2EsApp
 * -env dev
 *
 * @author zhangxuecheng4441
 * @date 2023/2/22/022 11:43
 */
public class FlinkStreamDemoApp extends FlinkStreamModel{


    /**
     * idea 启动需要配置 [add dependencies with "provided"]
     * @param args args
     * @throws Exception Exception
     */
    public static void main(String[] args) throws Exception {
        consumeKafka(args);
    }

    private static void consumeKafka(String[] args) throws Exception {
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


    void process(){
        ////kafka ds
        //val kafkaSource = kafkaSource("test", "test01").setParallelism(2);
        //
        ////json 字段解析
        //val checkResultStream = kafkaSource.process(new JsonStrCheckFunc<>(PersonnelInfo.class, errorDataTag))
        //        .returns(TypeInformation.of(PersonnelInfo.class)).setParallelism(1).name("Check Json Result");
        //
        //val personEsSink = EsSink.<PersonnelInfo>builder()
        //        .esSinkFunc(new GenericEsFunc<>(index))
        //        .config(config).build().getSink();
        //
        ////数据写es
        //checkResultStream.addSink(personEsSink).setParallelism(4).name("Person Es Write");
        //
        //val errorSink = KafkaSink.<String>builder()
        //        .setBootstrapServers(config.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
        //        .setRecordSerializer(
        //                KafkaRecordSerializationSchema.builder()
        //                        .setTopic(errorDataTag)
        //                        .setValueSerializationSchema(new SimpleStringSchema())
        //                        .build()
        //        )
        //        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        //        //.setKafkaProducerConfig(KafkaUtil.defaultProducerProp())
        //        .build();
        //
        ////脏数据写Kafka
        //checkResultStream.getSideOutput(new OutputTag<>(errorDataTag,TypeInformation.of(String.class)))
        //        .sinkTo(errorSink).setParallelism(2).name("Error Massage Sink");
        //
        //FlinkStreamModel.env.execute("Personnel Kafka Output Es");
    }
}
