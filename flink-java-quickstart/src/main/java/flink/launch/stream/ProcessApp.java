package flink.launch.stream;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.RandomUtil;
import flink.function.check.JsonStrCheckFunc;
import flink.model.FlinkStreamModel;
import flink.pojo.AccountUploadPojo;
import flink.sink.GenericSink;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.OutputTag;

import java.util.List;

/**
 * 一堆数据 需要将其中数据分成多个类别 进行再处理使用旁路
 * 用例: 读取数据找到正确的json数据打印 不正确的输出
 * todo IDEA run main方法时 need [add IDEA provided选项 ] AND [add -local local to program argument]
 *
 * @author zhangxuecheng4441
 * @date 2022/3/13/013 15:21
 */
@Slf4j
public class ProcessApp extends FlinkStreamModel {


    public static void main(String[] args) throws Exception {
        initEnv(args);
        val errorTag = "error";

        //造假数据
        val source = env.addSource(new SourceFunction<String>() {
            boolean out = true;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                val strList = ListUtil.toList("{\"uid\":1}", "1", "2");
                while (out) {
                    ThreadUtil.sleep(1.2 * 1000);
                    val str = strList.get(RandomUtil.randomInt(0, 3));
                    log.warn("add string:{}", str);
                    sourceContext.collect(str);
                }
            }

            @Override
            public void cancel() {
                out = false;
            }
        }).setParallelism(1).name("string-source");


        //分流 判断json直接返回 非json添加到名为errorTag的流中
        val process = source.process(new JsonStrCheckFunc<>(AccountUploadPojo.class, errorTag))
                .returns(TypeInformation.of(AccountUploadPojo.class))
                .setParallelism(2).name("json-parse");

        //处理直接返回的数据
        process.map(String::valueOf)
                .addSink(new GenericSink<String>(1) {
                    @Override
                    public void flush(List<String> elements) {
                        log.warn(">>>>>> right str:{}", elements);
                    }
                }).setParallelism(1).name("right-sink");

        //获取名为errorTag流中的数据处理
        process.getSideOutput(new OutputTag<>(errorTag, TypeInformation.of(String.class)))
                        .addSink(new GenericSink<String>(1) {
                            @Override
                            public void flush(List<String> elements) {
                                log.error(">>>>>> error str:{}", elements);
                            }
                        }).setParallelism(1).name("error-sink");

        env.execute("ProcessApp");
    }
}
