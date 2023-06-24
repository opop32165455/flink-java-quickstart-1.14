package flink.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 通用output模板
 *
 * @author zhangxuecheng4441
 * @date 2022/2/20/020 15:04
 */
@Slf4j
public abstract class GenericAbstractOutputFormat<T> extends RichOutputFormat<T> {
    private static final long serialVersionUID = 3552174055171640294L;

    public final List<T> dataCache;

    private final int batchInterval;
    private int batchCount = 0;

    @Override
    public void configure(Configuration parameters) {
        //todo 执行一次 可以在此处 getRuntimeContext()获取全局配置
    }

    public GenericAbstractOutputFormat(int batchInterval) {
        this.batchInterval = batchInterval;
        dataCache = new ArrayList<>();
    }

    public GenericAbstractOutputFormat() {
        this.batchInterval = 1000;
        dataCache = new ArrayList<>();
    }


    @Override
    public void open(int taskNumber, int numTasks) {
        //todo 可以在此处获取配置
    }


    @Override
    public void writeRecord(T record) {
        if (record != null) {
            this.addBatch(record);

            ++this.batchCount;
            if (this.batchCount >= this.batchInterval) {
                this.flush(dataCache);
                dataCache.clear();
                this.batchCount = 0;
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (!this.dataCache.isEmpty()) {
            try {
                this.flush(dataCache);
            } finally {
                this.dataCache.clear();
            }
        }
    }

    /**
     * 增加批量数据 缓存批写
     *
     * @param element element
     */
    public void addBatch(T element) {
        dataCache.add(element);
    }

    /**
     * 数据入库
     *
     * @param dataCache 数据
     */
    public abstract void flush(List<T> dataCache);
}
