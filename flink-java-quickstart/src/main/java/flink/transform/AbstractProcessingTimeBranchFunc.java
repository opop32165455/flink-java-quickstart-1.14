package flink.transform;

import cn.hutool.core.date.DateUtil;
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
 * 以ProcessingTime作为时间依据 数据攒批处理 当达到{@code maxBufferSize}处理数据 或者时间达到{@code bufferTimeoutSec}处理数据
 * 将<K,IN>类型数据数据攒到一定数量 输出为OUT
 * 可以通过重写branchTransform修改逻辑
 *
 * @author zhangxuecheng
 * @package flink.transform.windowing
 * @className BranchProcessFunc
 * @description 数据攒批处理
 * @date 2023/6/29 17:47
 */
@Slf4j
public abstract class AbstractProcessingTimeBranchFunc<K, IN, OUT> extends KeyedProcessFunction<K, IN, OUT> {
    private static final long serialVersionUID = -2053123841723352389L;
    private final int maxBufferSize;
    private final int bufferTimeoutSec;
    private transient ListState<IN> buffer;
    private transient ValueState<Long> timeState;
    private final TypeInformation<IN> typeInfo;

    public AbstractProcessingTimeBranchFunc(int maxBufferSize, int bufferTimeoutSec, TypeInformation<IN> typeInfo) {
        this.bufferTimeoutSec = bufferTimeoutSec;
        this.maxBufferSize = maxBufferSize;
        this.typeInfo = typeInfo;
    }

    @Override
    public void open(Configuration parameters) {
        val runtimeContext = getRuntimeContext();

        //branch buffer
        ListStateDescriptor<IN> batchStateDescriptor = new ListStateDescriptor<>("batchState", typeInfo);
        buffer = runtimeContext.getListState(batchStateDescriptor);

        //上一次触发时间
        ValueStateDescriptor<Long> lastTriggerTimeDescriptor = new ValueStateDescriptor<>("lastTriggerTime", Long.class);
        timeState = runtimeContext.getState(lastTriggerTimeDescriptor);

        //准备操作
        openPrepare();
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<K, IN, OUT>.OnTimerContext ctx, Collector<OUT> out) throws Exception {
        var timerService = ctx.timerService();
        K key = ctx.getCurrentKey();
        long timeOffset = timerService.currentProcessingTime() - timeState.value();
        log.info(">>>>>>>> time check {} - {}", DateUtil.date(timerService.currentProcessingTime()),DateUtil.date(timeState.value()));

        //允许时间上有0.1s误差
        long tolerateTime = 100L;
        log.info(">>>>>>>> time offset {}ms",timeOffset);
        if (timeOffset >= bufferTimeoutSec * 1000L - tolerateTime) {
            log.error(">>>>>>>> time offset processBuffer {}ms",timeOffset);
            // 处理剩余的批次数据
            processBuffer(key, out);
            timeState.update(timerService.currentProcessingTime());
        }
    }


    @Override
    public void processElement(IN value, KeyedProcessFunction<K, IN, OUT>.Context ctx, Collector<OUT> out) throws Exception {
        //初始化时间
        initTimeState(ctx);

        val key = ctx.getCurrentKey();

        //设置这个数据最晚要被处理的时间
        setNextExecuteTime(ctx.timerService());

        //添加数据到批次状态
        buffer.add(value);

        // 当批次大小达到阈值或距离上一次处理时间超过阈值时，触发处理
        if (buffer.get().spliterator().getExactSizeIfKnown() >= maxBufferSize) {
            // 处理批次数据
            processBuffer(key, out);
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
            log.info("check init time:{}",DateUtil.date(context.timerService().currentProcessingTime()));
            timeState.update(context.timerService().currentProcessingTime());
        }
    }

    /**
     * 批量数据处理
     *
     * @param out buffer数据操作
     * @throws Exception Exception
     */
    private void processBuffer(K key, Collector<OUT> out) throws Exception {
        //定时器OnTime 和 攒批处理processElement 是顺序执行 不会有并发问题 不需要加锁
        List<IN> batchData = new ArrayList<>();
        for (IN item : buffer.get()) {
            batchData.add(item);
        }

        if (!batchData.isEmpty()) {
            branchTransform(out, key, batchData);
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
    public abstract void branchTransform(Collector<OUT> out, K key, List<IN> batchData);


    /**
     * 预留方法 可在runtime中获取相应数据
     */
    public void openPrepare() {

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