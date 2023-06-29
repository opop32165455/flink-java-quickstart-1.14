package flink.launch.stream.sql;

import flink.model.FlinkModel;
import flink.model.FlinkStreamModel;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;

/**
 * @author zhangxuecheng
 * @package flink.launch.stream.sql
 * @className SqlLauncher
 * @description sql launcher todo 有问题 建议还是使用table api
 * @date 2023/6/27 8:59
 */
@Slf4j
public class SqlLauncher extends FlinkStreamModel {
    public static void main(String[] args) throws Exception {
        // 创建 Flink SQL Client 的配置
        Configuration configuration = new Configuration();

        // 设置 Flink SQL Client 的相关属性
        val resourceStr = FlinkModel.getResourceStr("flink/sql/demo/DemoSqlSelect.sql");

        // 创建 Flink SQL Client
        CliFrontend cli = new CliFrontend(configuration,new ArrayList<>());

        // 解析参数
        String[] sqlArgs = {"-e", resourceStr};
        cli.parseAndRun(sqlArgs);



    }


}