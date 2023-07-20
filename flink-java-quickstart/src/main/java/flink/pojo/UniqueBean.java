package flink.pojo;

/**
 * 指定唯一属性
 *
 * @author zhangxuecheng
 * @package flink.pojo
 * @className UniqueBean
 * @description 数据攒批处理
 * @date 2023/6/29 17:47
 */
public interface UniqueBean {
    /**
     * 这个确定这个bean 唯一的key
     *
     * @return String
     */
    String uniqueKey();
}
