package flink.model;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * flink 流任务模板
 *
 * @author zhangxuecheng4441
 * @date 2022/10/31/031 19:36
 */
@Slf4j
public class FlinkStreamModel implements FlinkModel {

    /**
     * environment
     */
    public static StreamExecutionEnvironment env;
    /**
     * cli args
     */
    public static MultipleParameterTool param;
    /**
     * default config
     */
    public static Map<String, String> config;

    /**
     * @param args params
     * @return StreamExecutionEnvironment
     */
    public static StreamExecutionEnvironment initEnv(String[] args) throws IOException {
        //get params
        val params = MultipleParameterTool.fromArgs(args);
        param = params;

        long checkpointInterval = params.getLong(CHECKPOINT_INTERVAL, 120 * 1000);
        String rocksDbPath = params.get(ROCKS_DB_PATH, "/data0/flink/rocksDb,/data1/flink/rocksDb");
        int parallelism = params.getInt(PARALLELISM, 2);
        // set up the execution environment
        env = StreamEnvBuilder.builder()
                .setCheckpointInterval(checkpointInterval)
                .setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
                .setCheckpointTimeout(600 * 1000L)
                .setMinPauseBetweenCheckpoints(5 * 1000)
                .setTolerableCheckpointFailureNumber(2)
                .setMaxConcurrentCheckpoints(1)
                .setDefaultRestartStrategy(3, Time.of(3, TimeUnit.MINUTES), Time.of(2, TimeUnit.MINUTES))
                .setParallelism(parallelism)
                .build();

        //关闭操作链
        if (params.getBoolean(DISABLE_OPERATOR_CHAINING, false)) {
            env.disableOperatorChaining();
        }
        //netty buffer 传输超时

        //开启rocksDb 增量checkpoint
        if (params.has(ROCKS_DB_PATH)) {
            env.setStateBackend(new RocksDBStateBackend(rocksDbPath, true));
        }

        //todo debug 增加参数 -local local 可以IDEA测试开启 http://localhost:8081/ 研发环境
        if (params.has(LOCAL_ENV_PARAM)) {
            env = StreamContextEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        }

        //批模式运行
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //根据环境 加载配置文件到 PropLoader
        String envConf = params.get(ENV_PARAM, DEV_ENV);
        ParameterTool parameterTool = FlinkModel.getInitConfig(envConf);

        config = parameterTool.toMap();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        return env;
    }


}
