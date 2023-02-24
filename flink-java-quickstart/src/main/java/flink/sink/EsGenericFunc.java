package flink.sink;

import cn.hutool.core.bean.BeanUtil;
import flink.pojo.EsDoc;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.client.Requests;

/**
 * @author zhangxuecheng4441
 * @date 2022/10/12/012 20:51
 */

@Slf4j
@Builder
@AllArgsConstructor
public class EsGenericFunc<T extends EsDoc> implements ElasticsearchSinkFunction<T> {

    private static final long serialVersionUID = -7112213300953549422L;

    private String index;


    @Override
    public void open() throws Exception {
        ElasticsearchSinkFunction.super.open();
    }

    @Override
    public void close() throws Exception {
        ElasticsearchSinkFunction.super.close();
    }

    @Override
    public void process(T element, RuntimeContext runtimeContext, RequestIndexer indexer) {
        val elementMap = BeanUtil.beanToMap(element, false, false);
        indexer.add(Requests.indexRequest()
                .index(index)
                .id(element.getDocId())
                .routing(element.getRouting())
                .source(elementMap));
    }
}
