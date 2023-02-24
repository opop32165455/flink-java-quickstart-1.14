package flink.model;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.Map;

/**
 * 批任务模板
 * Flink目标通过有限源+流 替代批任务
 * 批任务不支持checkpoint
 *
 * @author zhangxuecheng4441
 * @date 2022/10/31/031 19:36
 */
@Slf4j
public class FlinkBranchModel implements FlinkModel {
    /**
     * environment
     */
    public static ExecutionEnvironment env;
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
     * @throws IOException IOException
     */
    public static ExecutionEnvironment branchEnv(String[] args) throws IOException {
        //get params
        val params = MultipleParameterTool.fromArgs(args);

        param = params;

        int parallelism = params.getInt("parallelism", 2);
        // set up the execution environment
        env = ExecutionEnvironment.getExecutionEnvironment();

        //todo debug IDEA测试开启 http://localhost:8081/ 研发环境
        if (DEV_ENV.equals(params.get(ENV_PARAM))) {
            env = ExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        }

        env.setParallelism(parallelism);

        //根据环境 加载配置文件到 PropLoader
        String envConf = params.get(ENV_PARAM, DEV_ENV);
        ParameterTool parameterTool = FlinkModel.getInitConfig(envConf);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        //def config
        config = parameterTool.toMap();
        return env;
    }


}
