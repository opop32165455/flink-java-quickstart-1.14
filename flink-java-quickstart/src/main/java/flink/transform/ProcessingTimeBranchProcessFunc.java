package flink.transform;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * @author zhangxuecheng
 * @package flink.launch.stream.windowing
 * @className BranchProcessFunc
 * @description 以ProcessingTime作为时间依据 数据攒批处理
 * @date 2023/6/29 17:47
 */
@Slf4j
public class ProcessingTimeBranchProcessFunc<K, T> extends KeyedProcessFunction<K, T, List<T>> {
    private final int maxBufferSize;
    private final int bufferTimeoutSec;
    private transient ListState<T> buffer;
    private transient ValueState<Long> timeState;
    private final TypeInformation<T> typeInfo;

    public ProcessingTimeBranchProcessFunc(int maxBufferSize, int bufferTimeoutSec, TypeInformation<T> typeInfo) {
        this.bufferTimeoutSec = bufferTimeoutSec;
        this.maxBufferSize = maxBufferSize;
        this.typeInfo = typeInfo;
    }

    @Override
    public void open(Configuration parameters) {
        val runtimeContext = getRuntimeContext();

        //branch buffer
        ListStateDescriptor<T> batchStateDescriptor = new ListStateDescriptor<>("batchState", typeInfo);
        buffer = runtimeContext.getListState(batchStateDescriptor);

        //上一次触发时间
        ValueStateDescriptor<Long> lastTriggerTimeDescriptor = new ValueStateDescriptor<>("lastTriggerTime", Long.class);
        timeState = runtimeContext.getState(lastTriggerTimeDescriptor);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<K, T, List<T>>.OnTimerContext ctx, Collector<List<T>> out) throws Exception {
        var timerService = ctx.timerService();
        long timeOffset = timerService.currentProcessingTime() - timeState.value();

        //允许时间上有0.1s误差
        long tolerateTime = 100L;
        if (timeOffset >= bufferTimeoutSec * 1000L - tolerateTime) {
            // 处理剩余的批次数据
            processBuffer(out);
            timeState.update(timerService.currentProcessingTime());
        }
    }


    @Override
    public void processElement(T value, KeyedProcessFunction<K, T, List<T>>.Context ctx, Collector<List<T>> out) throws Exception {
        //初始化时间
        initTimeState(ctx);

        //设置这个数据最晚要被处理的时间
        setNextExecuteTime(ctx.timerService());

        //添加数据到批次状态
        buffer.add(value);

        // 当批次大小达到阈值或距离上一次处理时间超过阈值时，触发处理
        if (buffer.get().spliterator().getExactSizeIfKnown() >= maxBufferSize) {
            // 处理批次数据
            processBuffer(out);
            timeState.update(ctx.timerService().currentProcessingTime());
        }
    }

    /**
     * 设置time state初始值
     *
     * @param context context
     * @throws IOException IOException
     */
    private void initTimeState(Context context) throws IOException {
        if (timeState.value() == null) {
            timeState.update(context.timerService().currentProcessingTime());
        }
    }

    /**
     * 批量数据处理
     *
     * @param out buffer数据操作
     * @throws Exception Exception
     */
    private void processBuffer(Collector<List<T>> out) throws Exception {
        //定时器OnTime 和 攒批处理processElement 是顺序执行 不会有并发问题 不需要加锁
        List<T> batchData = new ArrayList<>();
        for (T item : buffer.get()) {
            batchData.add(item);
        }

        if (!batchData.isEmpty()) {
            branchTransform(out, batchData);
        }
        // 清空批次状态
        buffer.clear();
    }

    /**
     * 批量数据处理
     * 可重写方法 修改具体数据处理
     *
     * @param out       Collector
     * @param batchData batchData
     */
    public void branchTransform(Collector<List<T>> out, List<T> batchData) {
        out.collect(batchData);
    }


    /**
     * 设置 数据下次处理时间 并注册定时器
     *
     * @param timerService timerService
     */
    private void setNextExecuteTime(TimerService timerService) {
        long timerTimestamp = timerService.currentProcessingTime() + bufferTimeoutSec * 1000L;
        timerService.registerProcessingTimeTimer(timerTimestamp);
    }

}