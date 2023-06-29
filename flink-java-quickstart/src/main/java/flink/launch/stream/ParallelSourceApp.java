package flink.launch.stream;

import flink.model.FlinkStreamModel;
import flink.source.GenericRichParallelSourceFunction;
import lombok.val;

/**
 * @author zhangxuecheng
 * @package flink.launch.stream
 * @className ParallelSourceApp
 * @description Parallel Source Demo
 * @date 2023/6/25 11:51
 */
public class ParallelSourceApp extends FlinkStreamModel {
    public static void main(String[] args) throws Exception{
        initEnv(args);

        val stringSource = env.addSource(new GenericRichParallelSourceFunction<String>() {
            @Override
            public String createData() {
                return generator.nextHexString(10);
            }
        }).setParallelism(4);


        stringSource.print();

        env.execute("parallel-demo");

    }
}