package flink.source;

import cn.hutool.core.date.DateUtil;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;

import javax.annotation.Nullable;
import java.util.Date;

/**
 * @author zhangxuecheng
 * @package flink.source
 * @className TestDataGeneratorSource
 * @description Data Generator Source To Test
 * @date 2023/6/26 9:43
 */
public class TestDataGeneratorSource extends DataGeneratorSource<Tuple3<String, Integer, Date>> {
    static DataGenerator<Tuple3<String, Integer, Date>> defaultDataGenerator = new DataGenerator<Tuple3<String, Integer, Date>>() {
        RandomDataGenerator generator;

        @Override
        public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {
            generator = new RandomDataGenerator();
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple3<String, Integer, Date> next() {
            return Tuple3.of(generator.nextHexString(10), generator.nextInt(1, 10), DateUtil.offsetHour(DateUtil.date(), -1));
        }
    };

    public TestDataGeneratorSource(int rowsPerSecond, @Nullable Integer numberOfRows) {
        super(defaultDataGenerator, rowsPerSecond, Long.valueOf(numberOfRows));
    }

}