package flink.transform;

import cn.hutool.json.JSONObject;

/**
 * @author zhangxuecheng
 * @package flink.transform
 * @className BranchJoinDorisFunc
 * @description 攒批 join查询doris 获取数据
 * @date 2023/7/19 17:25
 */
public interface DimJoin<T> {

    /**
     *
     * @return key name[]
     */
    String[] getKeyName();

    /**
     * getKey
     * @param input input
     * @return key value
     */
    Object[] getKey(T input);

    /**
     * 关联事实数据和维度数据
     * @param input data
     * @param dimInfo result
     * @throws Exception Exception
     */

    void join(T input, JSONObject dimInfo) throws Exception;
}
