package flink.model;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author zhangxuecheng4441
 * @date 2022/2/24/024 17:31
 */
public interface FlinkModel {
    String ENV_PARAM = "env";
    String DEV_ENV = "dev";
    String LOCAL_ENV_PARAM = "local";

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
}
