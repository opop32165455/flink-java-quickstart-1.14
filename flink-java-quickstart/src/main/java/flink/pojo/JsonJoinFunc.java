package flink.pojo;

/**
 * 指定唯一属性
 *
 * @author zhangxuecheng
 * @package flink.pojo
 * @className JsonJoinFunc
 * @description 数据攒批处理
 * @date 2023/7/19 17:47
 */
@FunctionalInterface
public interface JsonJoinFunc<E, JSON> {

    /**
     * 为element join json的数据
     *
     * @param element element
     * @param json json
     */
    void join(E element, JSON json);
}