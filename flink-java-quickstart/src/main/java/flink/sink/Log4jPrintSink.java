package flink.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author zhangxuecheng
 * @package flink.sink
 * @className Log4jPrintSink
 * @description log4j print
 * @date 2023/7/20 14:47
 */
@Slf4j
public class Log4jPrintSink<T> extends RichSinkFunction<T> {
    @Override
    public void invoke(T value, Context context) throws Exception {
        log.warn("log print >>>> {}", String.valueOf(value));
    }
}