package flink.launch;

import cn.hutool.core.thread.ThreadUtil;
import flink.model.FlinkBranchModel;
import flink.sink.GenericOutPutFormat;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.io.GenericInputFormat;

import java.io.IOException;
import java.util.List;

/**
 * todo IDEA启动main方法时 需要勾选IDE的provided选项 增加-local local到启动参数
 *
 * @author zhangxuecheng4441
 * @date 2023/2/22/022 11:43
 */
@Slf4j
public class FlinkBranchDemoApp extends FlinkBranchModel {


    /**
     * todo IDEA启动main方法时 需要勾选IDE的 [add dependencies with "provided"] 增加-local local到启动参数
     *
     * @param args args
     * @throws Exception Exception
     */
    public static void main(String[] args) throws Exception {
        initEnv(args);

        //获取数据
        val inputSource = env.createInput(new GenericInputFormat<String>() {
            final int limit = 22;
            boolean isEnd = false;
            int printCount = 0;

            @Override
            public boolean reachedEnd() throws IOException {
                return isEnd;
            }

            @Override
            public String nextRecord(String reuse) throws IOException {
                printCount++;
                if (printCount >= limit) {
                    isEnd = true;
                }
                ThreadUtil.sleep(1.2 * 1000);
                log.warn("add-source-" + printCount);
                return "print-" + printCount;
            }
        }).setParallelism(1).name("string-source");

        //每获取5个数据输出一次数据
        inputSource.output(new GenericOutPutFormat<String>(5) {
            @Override
            public void flush(List<String> elements) {
                ThreadUtil.sleep(3.5 * 1000);
                log.error("map print:{}", elements);
            }
        }).setParallelism(2).name("string-source");

        //todo debug 增加参数 -local local 可以IDEA测试开启 http://localhost:{端口打印在日志}/ 研发环境
        env.execute("DemoBranchApp");
    }


}
