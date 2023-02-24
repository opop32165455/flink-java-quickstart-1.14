package flink.model;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
    public static StreamExecutionEnvironment initEnv(String[] args) {
        //get params
        val params = MultipleParameterTool.fromArgs(args);
        param = params;

        long checkpointInterval = params.getLong("checkpointInterval", 120 * 1000);
        int parallelism = params.getInt("parallelism", 4);
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

        //todo debug IDEA测试开启 http://localhost:8081/ 研发环境
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
