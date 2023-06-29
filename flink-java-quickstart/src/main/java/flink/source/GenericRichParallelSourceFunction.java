package flink.source;

import cn.hutool.core.thread.ThreadUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author zhangxuecheng
 * @package flink.source
 * @className GenericRichParallelSourceFunction
 * @description Parallel Source Function
 * @date 2023/6/25 11:43
 */
@Slf4j
public abstract class GenericRichParallelSourceFunction<T> extends RichParallelSourceFunction<T> {

    public RandomDataGenerator generator;
    public boolean isRunning = false;

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        while (isRunning) {
            int attemptNumber = getRuntimeContext().getAttemptNumber();
            log.info("AttemptNumber is {}", attemptNumber);
            ThreadUtil.sleep(1000L);
            ctx.collect(createData());
        }
    }

    public abstract T createData();

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);
    }

    @Override
    public RuntimeContext getRuntimeContext() {

        return super.getRuntimeContext();
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        return super.getIterationRuntimeContext();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        generator = new RandomDataGenerator();
        isRunning = true;
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
        isRunning = false;
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}