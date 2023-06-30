package flink.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author zhangxuecheng
 * @package flink.sink
 * @className GenericAbstractSink
 * @description generic sink
 * @date 2023/6/30 10:10
 */
public abstract class GenericAbstractSink<T> extends RichSinkFunction<T> {

    @Override
    public void invoke(T value, Context context) throws Exception {
       flush(value);
    }

    /**
     * 数据入库
     *
     * @param element elements
     */
    public abstract void flush(T element);
}