package flink.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangxuecheng4441
 * @date 2022/10/14/014 15:25
 */
public abstract class GenericAbstractBranchSink<T> extends RichSinkFunction<T> {
    private static final long serialVersionUID = -7594648194757224332L;

    public final List<T> dataCache;
    private final int batchInterval;
    private int batchCount = 0;

    public GenericAbstractBranchSink(int batchInterval) {
        this.batchInterval = batchInterval;
        dataCache = new ArrayList<>();
    }


    @Override
    public void invoke(T record, Context context) {
        if (record != null) {
            ++this.batchCount;
            this.addBatch(record);

            if (this.batchCount >= this.batchInterval) {
                this.flush(dataCache);
                dataCache.clear();
                this.batchCount = 0;
            }
        }
    }

    /**
     * 数据入库
     *
     * @param element elements
     */
    public abstract void flush(List<T> element);

    /**
     * 增加批量数据 缓存批写
     *
     * @param element element
     */
    public void addBatch(T element) {
        dataCache.add(element);
    }


    @Override
    public void close() throws Exception {
        if (!this.dataCache.isEmpty()) {
            try {
                this.flush(dataCache);
            } finally {
                this.dataCache.clear();
            }
        }
        super.close();
    }
}
