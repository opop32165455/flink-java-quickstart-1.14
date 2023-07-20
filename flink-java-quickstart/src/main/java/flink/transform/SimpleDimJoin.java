package flink.transform;


import com.alibaba.fastjson2.JSONObject;

/**
 * @author zhangxuecheng
 * @package flink.transform
 * @className BranchJoinDorisFunc
 * @description 攒批 join查询doris 获取数据
 * @date 2023/7/19 17:25
 */
public interface SimpleDimJoin<T> {

    /**
     * @return key
     */
    String getKeyName();

    /**
     * getKey
     *
     * @param input input
     * @return key value
     */
    Object getKey(T input);

    /**
     * 关联事实数据和维度数据
     *
     * @param input   data
     * @param dimInfo result
     */

    void join(T input, JSONObject dimInfo);
}
