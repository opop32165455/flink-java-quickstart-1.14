package flink.source;

import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import flink.pojo.AccountUploadPojo;
import flink.utils.OssHandle;
import lombok.AllArgsConstructor;
import lombok.val;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.core.io.GenericInputSplit;

import java.io.IOException;
import java.util.List;

/**
 * @author zhangxuecheng4441
 * @date 2022/10/11/011 17:48
 */
@AllArgsConstructor
public class ExcelInputFormat extends GenericInputFormat<List<AccountUploadPojo>> {
    public ExcelInputFormat(String config) {
        this.config = config;
    }

    private String  config;
    private final static int START_ROW = 2;
    private final static int HEAD_ROW = 1;
    private final static int READ_SHEET = 0;
    private final static int BRANCH_SIZE = 2000;
    private OssHandle ossHandle;
    private int rowCount;
    private int readStart;
    private int readEnd;
    private ExcelReader reader;
    protected boolean endReached;

    @Override
    public void open(GenericInputSplit split) throws IOException {
        super.open(split);
        RuntimeContext context = getRuntimeContext();
        ExecutionConfig.GlobalJobParameters globalParams = context.getExecutionConfig().getGlobalJobParameters();
        val globalConf =  globalParams.toMap();

        //从oss中获取web上传的文件 todo
        ossHandle = null;

        //或取流
        val ins = ossHandle.downloadStream(config);
        reader = ExcelUtil.getReader(ins, READ_SHEET);

        //获取总行数
        rowCount = reader.getRowCount();
        readStart = START_ROW;
    }

    @Override
    public boolean reachedEnd() {
        return endReached;
    }

    @Override
    public List<AccountUploadPojo> nextRecord(List<AccountUploadPojo> reuse) throws IOException {
        if (readEnd < rowCount) {
            readEnd = readStart + BRANCH_SIZE - 1;
            List<AccountUploadPojo> readRecord = reader.read(HEAD_ROW, readStart, readEnd, AccountUploadPojo.class);
            readStart = readEnd + 1;
            return readRecord;
        }
        endReached = true;
        return null;
    }
}
