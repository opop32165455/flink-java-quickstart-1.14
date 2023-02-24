package flink.pojo;

/**
 * Es写入对象规范
 * @author zhangxuecheng4441
 * @date 2023/2/24/024 17:24
 */
public interface EsDoc {
    /**
     * 获取id
     *
     * @return id
     */
    String getDocId();

    /**
     * 获取routing
     *
     * @return routing
     */
    String getRouting();
}
