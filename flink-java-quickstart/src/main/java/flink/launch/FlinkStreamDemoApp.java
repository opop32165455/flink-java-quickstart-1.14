package flink.launch;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.thread.ThreadUtil;
import flink.model.FlinkStreamModel;
import flink.sink.GenericSink;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;

/**
 * todo IDEA启动main方法时 需要勾选IDE的provided选项 增加-local local到启动参数
 *
 * @author zhangxuecheng4441
 * @date 2023/2/22/022 11:43
 */
@Slf4j
public class FlinkStreamDemoApp extends FlinkStreamModel {


    /**
     * idea 启动需要配置 [add dependencies with "provided"]
     *
     * @param args args
     * @throws Exception Exception
     */
    public static void main(String[] args) throws Exception {
        initEnv(args);

        //获取数据源
        val source = env.addSource(new SourceFunction<String>() {
            boolean out = true;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (out) {
                    ThreadUtil.sleep(1.2 * 1000);
                    val str = "print-time:" + DateUtil.now();
                    log.warn("add string:{}", str);
                    sourceContext.collect(str);
                }
            }

            @Override
            public void cancel() {
                out = false;
            }
        }).setParallelism(1).name("string-source");

        //打印
        source.print().setParallelism(2).name("print-time");

        //每5个数据进行一次数据输出
        source.addSink(new GenericSink<String>(5) {
            @Override
            public void flush(List<String> elements) {
                log.error("output str:{}", elements);
            }
        });

        //todo debug 增加参数 -local local 可以IDEA测试开启 http://localhost:8081/ 研发环境
        env.execute("DemoStreamApp");
    }


    void process() {
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
