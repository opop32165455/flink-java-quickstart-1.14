package flink.launch;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.thread.ThreadUtil;
import flink.model.FlinkStreamModel;
import flink.sink.GenericAbstractSink;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;

/**
 * flink stream demo
 * todo IDEA run main方法时 need [add IDEA provided选项 ] AND [add -local local to program argument]
 *
 * @author zhangxuecheng4441
 * @date 2022/2/22/022 11:43
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
        var env = initEnv(args);

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
        source.addSink(new GenericAbstractSink<String>(5) {
            @Override
            public void flush(List<String> elements) {
                log.error("output str:{}", elements);
            }
        }).setParallelism(4).name("stream-sink");

        //todo debug 增加参数 -local local 可以IDEA测试开启 http://localhost:8081/ 研发环境
        FlinkStreamModel.env.execute("DemoStreamApp");
    }
}
