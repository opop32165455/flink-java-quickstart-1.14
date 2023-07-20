package flink.transform;

import cn.hutool.core.collection.CollUtil;
import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author zhangxuecheng
 * @package flink.transform
 * @className BranchJoinDorisFunc
 * @description 攒批 join查询doris 获取数据
 * @date 2023/7/19 17:25
 */
@Slf4j
public abstract class BranchJoinDorisFunc<K, IN> extends AbstractProcessingTimeBranchFunc<K, IN, IN> implements DimJoin<IN> {
    public static final String AND = "and";
    public static final String OR = "or";
    public static final String EQUALS = "=";
    public static final String P_LEFT = "(";
    public static final String P_RIGHT = ")";

    @Override
    public void open(Configuration parameters) {
        super.open(parameters);
        //模拟 获取doris连接池
        getRuntimeContext();
    }

    public BranchJoinDorisFunc(int maxBufferSize, int bufferTimeoutSec, TypeInformation<IN> typeInfo) {
        super(maxBufferSize, bufferTimeoutSec, typeInfo);
    }

    /**
     * 攒批数据处理
     *
     * @param out       Collector
     * @param key       分区key 使用分区key 进行分区性能更优
     * @param batchData batchData
     */
    @Override
    public void branchTransform(Collector<IN> out, K key, List<IN> batchData) {

        //获取查询参数
        Map<Tuple2<List<String>, List<Object>>, List<IN>> paramsBranchData = batchData.stream()
                .map(element -> Tuple2.of(
                                //obj[] equals()没有重写
                                Tuple2.of(Arrays.asList(getKeyName()), Arrays.asList(getKey(element))),
                                element
                        )
                )
                .collect(Collectors.groupingBy(t -> t.f0, Collectors.mapping(t -> t.f1, Collectors.toList())));

        //t0:key name t1:key
        Set<Tuple2<List<String>, List<Object>>> paramsSet = paramsBranchData.keySet();

        //拼sql[( k1 = a and k2 = b),( k1 = a and k2 = b)]
        List<String> paramList = paramsSet.stream()
                .map(t -> {
                    var params = new ArrayList<>();
                    var keyNames = t.f0;
                    var keyList = t.f1;
                    for (int i = 0; i < keyNames.size(); i++) {
                        //k1 = a
                        var aEqualsB = keyNames.get(i).concat(EQUALS).concat(String.valueOf(keyList.get(i)));
                        params.add(aEqualsB);
                    }
                    // k1 = a and k2 = b
                    return CollUtil.join(params, sqlParamRela(), P_LEFT, P_RIGHT);
                })
                .collect(Collectors.toList());

        String joinSql = selectSql() + CollUtil.join(paramList, OR);
        log.info("select join sql execute:{}", joinSql);

        //todo 查询doris
        List<JSONObject> queryList = new ArrayList<>();

        //使用doris结果关联原数据

        //输出
    }

    /**
     * sql 参数之间的关系
     *
     * @return and or
     */
    public String sqlParamRela() {
        return AND;
    }


    public abstract String selectSql();


    public static void main(String[] args) {
        String[] strings = {"", "222"};
        List<String> list = Arrays.asList(strings);

        System.out.println("strings = " + list);
    }


}