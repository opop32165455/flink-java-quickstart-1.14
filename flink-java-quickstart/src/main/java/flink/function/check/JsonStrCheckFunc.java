package flink.function.check;

import cn.hutool.core.util.CharUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zhangxuecheng4441
 * @date 2023/2/22/022 14:02
 */
@Slf4j
@AllArgsConstructor
public class JsonStrCheckFunc<T> extends ProcessFunction<String, T> {
    private static final long serialVersionUID = 4585081931639764614L;

    private final Class<T> tClass;
    private final String errorTag;


    @Override
    public void processElement(String value, ProcessFunction<String, T>.Context ctx, Collector<T> out) throws Exception {
        try {
            //if (!JSON.isValid(value)) {
            //    throw new IllegalArgumentException("it is not a valid JSON. ");
            //}
            //T element = JSON.parseObject(value, tClass);

            //todo check
            //out.collect(element);
        } catch (Exception e) {
            log.error("error:{} and error data:{}", e.getMessage(), value, e);
            ctx.output(new OutputTag<>(errorTag, TypeInformation.of(String.class)), e.getMessage() + CharUtil.COLON + value);
        }
    }
}
