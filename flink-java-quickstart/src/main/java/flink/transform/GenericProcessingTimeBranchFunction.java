package flink.transform;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.List;


/**
 * 以ProcessingTime作为时间依据 数据攒批处理 将IN数据攒到一定数量 输出为Tuple2<K, List<IN>
 * 可以通过重写branchTransform修改逻辑
 *
 * @author zhangxuecheng
 * @package flink.transform.windowing
 * @className BranchProcessFunc
 * @description 数据攒批处理
 * @date 2023/6/29 17:47
 */
@Slf4j
public class GenericProcessingTimeBranchFunction<K, IN> extends AbstractProcessingTimeBranchFunc<K, IN, Tuple2<K, List<IN>>> {
    private static final long serialVersionUID = -7648476828310065264L;

    public GenericProcessingTimeBranchFunction(int maxBufferSize, int bufferTimeoutSec, TypeInformation<IN> typeInfo) {
        super(maxBufferSize, bufferTimeoutSec, typeInfo);
    }

    @Override
    public void open(Configuration parameters) {
        super.open(parameters);
    }

    /**
     * 批量数据处理
     * 可重写方法 修改具体数据处理
     *
     * @param out       Collector
     * @param batchData batchData
     */
    @Override
    public void branchTransform(Collector<Tuple2<K, List<IN>>> out, K key, List<IN> batchData) {

        //todo 报错 collect处理时 类型报错
        System.out.println("key = " + key);
        System.out.println("batchData = " + batchData);
        out.collect(Tuple2.of(key, batchData));
    }

}