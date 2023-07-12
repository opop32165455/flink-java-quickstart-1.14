package flink.source;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.RandomUtil;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.summarize.aggregation.CompensatedSum;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author zhangxuecheng
 * @package flink.source
 * @className TestDataGeneratorSource
 * @description Data Generator Source To Test
 * @date 2023/6/26 9:43
 */
@Slf4j
public class DateGenUtil {

    public static RandomDataGenerator randomDataGenerator = new RandomDataGenerator();

    /**
     * DataGeneratorSource create
     *
     * @param rowsPerSecond rowsPerSecond
     * @param numberOfRows  numberOfRows
     * @param supplier      data supplier
     * @param <T>           <T>
     * @return DataGeneratorSource
     */

    public static <T> DataGeneratorSource<T> getDataSource(Integer rowsPerSecond, Integer numberOfRows, TSupplier<T> supplier) {
        return new DataGeneratorSource<>(new DataGenerator<T>() {
            private static final long serialVersionUID = 1L;
            public RandomDataGenerator generator;

            @Override
            public void open(String name, FunctionInitializationContext context, RuntimeContext runtimeContext) throws Exception {
                generator = new RandomDataGenerator();
            }

            @Override
            public boolean hasNext() {
                return true;
            }

            public RandomDataGenerator getGenerator() {
                return generator;
            }

            @Override
            public T next() {
                return supplier.get();
            }
        }, rowsPerSecond, Long.valueOf(numberOfRows));
    }


    /**
     * while(ture) Source
     *
     * @param sleep    sleep
     * @param consumer data supplier
     * @param <T>      <T>
     */
    public static <T> SourceFunction<T> whileSource(Number sleep, TConsumer<SourceFunction.SourceContext<T>> consumer) {
        return new RichSourceFunction<T>() {
            boolean run = true;

            @Override
            public void run(SourceContext<T> ctx) throws Exception {
                while (run) {
                    consumer.accept(ctx);
                    ThreadUtil.sleep(sleep);
                }
            }

            @Override
            public void cancel() {
                run = false;
            }
        };
    }

    @FunctionalInterface
    public interface TSupplier<T> extends Serializable {

        /**
         * Gets a result.
         *
         * @return a result
         */
        T get();
    }


    @FunctionalInterface
    public interface TConsumer<T> extends Serializable{

        /**
         * Performs this operation on the given argument.
         *
         * @param t the input argument
         */
        void accept(T t);

        default TConsumer<T> andThen(TConsumer<? super T> after) {
            Objects.requireNonNull(after);
            return (T t) -> { accept(t); after.accept(t); };
        }
    }

}