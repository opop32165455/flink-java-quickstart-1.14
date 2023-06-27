package flink.model;

import cn.hutool.core.io.IoUtil;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

/**
 * @author zhangxuecheng4441
 * @date 2022/2/24/024 17:31
 */
public interface FlinkModel {
    String ENV_PARAM = "env";
    String DEV_ENV = "dev";
    String LOCAL_ENV_PARAM = "local";
    String DISABLE_OPERATOR_CHAINING = "disableOperatorChaining";
    String CHECKPOINT_INTERVAL = "checkpointInterval";
    String ROCKS_DB_PATH = "rocksDbPath";
    String PARALLELISM = "parallelism";

    /**
     * 获取resource下配置
     *
     * @param envConf envConf
     * @return ParameterTool
     */
    static ParameterTool getInitConfig(String envConf) {
        InputStream resourceAsStream = FlinkStreamModel.class.getResourceAsStream("/" + String.format("application-%s.properties", envConf));
        ParameterTool parameterTool = null;
        try {
            parameterTool = ParameterTool.fromPropertiesFile(resourceAsStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return parameterTool;
    }


    /**
     * 获取resource下配置
     *
     * @param resourcePath Resource Path
     * @return ParameterTool
     */
    static String getResourceStr(String resourcePath) {
        try {
            InputStream resourceAsStream = FlinkStreamModel.class.getResourceAsStream("/" + resourcePath);
            return IoUtil.read(resourceAsStream, Charset.defaultCharset());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
