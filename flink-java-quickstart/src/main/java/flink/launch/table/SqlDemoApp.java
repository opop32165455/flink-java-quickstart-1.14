package flink.launch.table;

import flink.model.FlinkStreamModel;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zhangxuecheng
 * @package flink.launch.stream.sql
 * @className SqlLauncher
 * @description sql launcher
 * @date 2023/6/27 8:59
 */
@Slf4j
public class SqlDemoApp extends FlinkStreamModel {
    public static void main(String[] args) throws Exception {
        initEnv(args);

        val tableEnvSettings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        val tEnv = StreamTableEnvironment.create(env, tableEnvSettings);

        // 创建输入表和输出表
        tEnv.executeSql("CREATE TABLE inputTable (id INT, name STRING, eventTime TIMESTAMP(3), proctime AS PROCTIME())" +
                " WITH ('connector' = 'datagen', 'rows-per-second'='2', 'fields.id.kind'='sequence', 'fields.id.start'='1', 'fields.id.end'='100')");

        tEnv.executeSql("CREATE TABLE outputTable (id INT, name STRING, eventTime TIMESTAMP(3), proctime AS PROCTIME())" +
                " WITH ('connector' = 'print')");

        //将inputTable数据输出到outputTable

//        val result = tEnv.sqlQuery("SELECT * FROM inputTable");
//        tEnv.toDataStream(result, Row.class).print();
//        tEnv.toDataStream(result).print();

//
        tEnv.executeSql("INSERT INTO outputTable (id,name,eventTime) SELECT id,name,eventTime FROM inputTable");


        // 执行作业并输出结果
        env.execute("DataGen Example");
    }


}