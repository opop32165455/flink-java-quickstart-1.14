package flink.transform;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSONObject;
import flink.pojo.UniqueBean;
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
public abstract class BranchSimpleJoinDorisFunc<IN extends UniqueBean> extends AbstractBranchFunc<IN, IN> implements SimpleDimJoin<IN> {
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

    public BranchSimpleJoinDorisFunc(int maxBufferSize, int bufferTimeoutSec, TypeInformation<IN> typeInfo) {
        super(maxBufferSize, bufferTimeoutSec, typeInfo);
    }

    /**
     * 攒批数据处理
     *
     * @param out       Collector
     * @param batchData batchData
     */
    @Override
    public void branchTransform(Collector<IN> out, List<IN> batchData) {
        //batchData group by 查询条件
        Map<String, List<IN>> paramsBranchData = batchData.stream()
                .map(element -> Tuple2.of(String.valueOf(getKey(element)), element))
                .collect(Collectors.groupingBy(t -> t.f0, Collectors.mapping(t -> t.f1, Collectors.toList())));

        String joinSql = sqlBuild(paramsBranchData);


        //todo 查询doris

        List<JSONObject> queryList = new ArrayList<>();
        JSONObject jsonObject = new JSONObject(MapUtil.of("name", 1));
        queryList.add(jsonObject);

        //使用doris结果关联原数据
        Map<String, JSONObject> resultMap = queryList.stream()
                .collect(Collectors.toMap(json -> json.getString(getKeyName()), json -> json, (k1, k2) -> k2));

        //输出
        resultMap.forEach((key, json) ->
                paramsBranchData.get(key)
                        .stream()
                        .peek(element -> join(element, json))
                        .forEach(out::collect)

        );
    }

    public String sqlBuild(Map<String, List<IN>> paramsBranchData) {
        //[a,b,c]
        Set<String> paramsSet = paramsBranchData.keySet();

        //sql
        String joinSql = selectSql().concat(" where ")
                .concat(getKeyName())
                .concat(" in ")
                .concat(CollUtil.join(paramsSet, StrUtil.COMMA, P_LEFT, P_RIGHT));
        log.info("select join sql execute:{}", joinSql);
        return joinSql;
    }

    public abstract String selectSql();


    public static void main(String[] args) {
        String[] strings = {"", "222"};
        List<String> list = Arrays.asList(strings);

        System.out.println("strings = " + list);
    }


}