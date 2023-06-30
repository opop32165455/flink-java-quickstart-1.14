package flink.launch.stream;

import cn.hutool.core.date.DateUtil;
import flink.model.FlinkStreamModel;
import flink.sink.GenericAbstractSink;
import flink.source.GenericRichParallelSourceFunction;
import flink.transform.AbstractProcessingTimeBranchFunc;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.List;

/**
 * @author zhangxuecheng
 * @package flink.launch.stream
 * @className BranchProcessApp
 * @description branch process source
 * @date 2023/6/29 17:44
 */
@Slf4j
public class BranchProcessApp extends FlinkStreamModel {

    public static void main(String[] args) throws Exception {
        initEnv(args);
        val keyedStream = env.addSource(new GenericRichParallelSourceFunction<Tuple3<String, Integer, Date>>(17) {
            @Override
            public Tuple3<String, Integer, Date> createData() {
                return Tuple3.of(generator.nextHexString(10), generator.nextInt(1, 4), DateUtil.offsetHour(DateUtil.date(), -1));
            }
        }).setParallelism(1).keyBy(t -> String.valueOf(t.f1));

        keyedStream.print();

        keyedStream.process(new AbstractProcessingTimeBranchFunc<String, Tuple3<String, Integer, Date>, List<Tuple3<String, Integer, Date>>>(3, 4, TypeInformation.of(new TypeHint<Tuple3<String, Integer, Date>>() {
        })) {
            @Override
            public void branchTransform(Collector<List<Tuple3<String, Integer, Date>>> out, String key, List<Tuple3<String, Integer, Date>> batchData) {
                out.collect(batchData);
            }
        }).setParallelism(4).addSink(new GenericAbstractSink<List<Tuple3<String, Integer, Date>>>() {

            @Override
            public void flush(List<Tuple3<String, Integer, Date>> element) {
                log.warn("flush data count:{} data:{}", element.size(), element);
            }
        });

        env.execute("execute branch demo");
    }
}