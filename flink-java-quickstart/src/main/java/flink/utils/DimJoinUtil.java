package flink.utils;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSONObject;
import flink.pojo.JsonJoinFunc;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author zhangxuecheng
 * @package flink.utils
 * @className DimJoinUtil
 * @description Dim join
 * @date 2023/7/20 14:14
 */
@Slf4j
public class DimJoinUtil {
    private static final String AND = "and";
    private static final String OR = "or";
    private static final String EQUALS = "=";
    private static final String P_LEFT = "(";
    private static final String P_RIGHT = ")";


    /**
     * 简单的select xx from table where xx in (a,b,c) join
     *
     * @param elements     需要批量join的原始数据
     * @param keyNameFunc  从原始数据中 获取join字段的方法
     * @param joinKeyName  被join表中的字段
     * @param selectSql    select xxx from table 使用的查询语句 ps：尽量少字段 减少内存压力
     * @param joinFunction 将json数据 join到原始数据
     * @param collector    产出结果处理器哦
     * @param <IN>         需要批量join的原始数据类型
     */
    public static <IN> void simpleLargeJoin(Iterable<IN> elements,
                                            Function<IN, Object> keyNameFunc,
                                            String joinKeyName,
                                            String selectSql,
                                            JsonJoinFunc<IN, JSONObject> joinFunction,
                                            Consumer<IN> collector) {

        int defaultSplit = 100;
        val inElement = CollUtil.newArrayList(elements);
        //防止拼接出来的sql 超过sql最长限制
        for (List<IN> subInElement : CollUtil.split(inElement, defaultSplit)) {
            simpleJoin(subInElement, keyNameFunc, joinKeyName, selectSql, joinFunction, collector);
        }
    }

    /**
     * 简单的select xx from table where xx in (a,b,c) join
     *
     * @param elements     需要批量join的原始数据
     * @param keyNameFunc  从原始数据中 获取join字段的方法
     * @param joinKeyName  被join表中的字段
     * @param selectSql    select xxx from table 使用的查询语句 ps：尽量少字段 减少内存压力
     * @param joinFunction 将json数据 join到原始数据
     * @param collector    产出结果处理器哦
     * @param <IN>         需要批量join的原始数据类型
     */
    public static <IN> void simpleLargeJoin(Collection<IN> elements,
                                            Function<IN, Object> keyNameFunc,
                                            String joinKeyName,
                                            String selectSql,
                                            JsonJoinFunc<IN, JSONObject> joinFunction,
                                            Consumer<IN> collector) {

        int defaultSplit = 100;
        //防止拼接出来的sql 超过sql最长限制
        for (List<IN> subInElement : CollUtil.split(elements, defaultSplit)) {
            simpleJoin(subInElement, keyNameFunc, joinKeyName, selectSql, joinFunction, collector);
        }
    }

    /**
     * 简单的select xx from table where xx in (a,b,c) join
     *
     * @param elements     需要批量join的原始数据
     * @param keyNameFunc  从原始数据中 获取join字段的方法
     * @param joinKeyName  被join表中的字段
     * @param selectSql    select xxx from table 使用的查询语句 ps：尽量少字段 减少内存压力
     * @param joinFunction 将json数据 join到原始数据
     * @param collector    产出结果处理器哦
     * @param <IN>         需要批量join的原始数据类型
     */
    public static <IN> void simpleJoin(Collection<IN> elements,
                                       Function<IN, Object> keyNameFunc,
                                       String joinKeyName,
                                       String selectSql,
                                       JsonJoinFunc<IN, JSONObject> joinFunction,
                                       Consumer<IN> collector) {
        if (elements == null) {
            return;
        }
        //获取查询参数
        Map<String, List<IN>> paramsBranchData = elements
                .stream()
                .map(element -> Tuple2.of(String.valueOf(keyNameFunc.apply(element)), element))
                .collect(Collectors.groupingBy(t -> t.f0, Collectors.mapping(t -> t.f1, Collectors.toList())));

        //[a,b,c]
        Set<String> paramsSet = paramsBranchData.keySet();

        //sql select a,b,c from where ${joinKeyName} in (${element.keyNameFunc})
        String joinSql = selectSql.concat(" where ")
                .concat(joinKeyName)
                .concat(" in ")
                .concat(P_LEFT)
                .concat(CollUtil.join(paramsSet, StrUtil.COMMA))
                .concat(P_RIGHT);
        log.info("select join sql execute:{}", joinSql);

        //todo 查询doris
        List<JSONObject> queryList = new ArrayList<>();
        JSONObject jsonObject = new JSONObject(MapUtil.of("name", 1));
        queryList.add(jsonObject);

        log.info("execute sql :{} \r\n result size:{}", joinSql, queryList.size());

        //使用doris结果关联原数据
        Map<String, JSONObject> resultMap = queryList.stream()
                .collect(Collectors.toMap(json -> json.getString(joinKeyName), json -> json, (k1, k2) -> k2));

        //输出
        paramsBranchData.forEach((key, keyedElements) -> {
                    JSONObject json = resultMap.get(key);
                    keyedElements.stream()
                            .peek(element -> {
                                if (json != null) {
                                    joinFunction.join(element, json);
                                }
                            })
                            .forEach(collector);
                }
        );
    }

}