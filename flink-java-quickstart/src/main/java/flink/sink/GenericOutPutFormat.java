package flink.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.RichOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 通用output模板
 *
 * @author zhangxuecheng4441
 * @date 2023/2/20/020 15:04
 */
@Slf4j
public abstract class GenericOutPutFormat<T> extends RichOutputFormat<T> {
    private static final long serialVersionUID = 3552174055171640294L;

    public final List<T> dataCache;

    private final int batchInterval;
    private int batchCount = 0;

    public GenericOutPutFormat(int batchInterval) {
        this.batchInterval = batchInterval;
        dataCache = new ArrayList<>();
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

    }


    @Override
    public void writeRecord(T record) {
        if (record != null) {
            this.addBatch(record);

            ++this.batchCount;
            if (this.batchCount >= this.batchInterval) {
                this.flush();
                dataCache.clear();
                this.batchCount = 0;
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (!this.dataCache.isEmpty()) {
            try {
                this.flush();
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
     */
    public abstract void flush();
}
