package flink.launch.table;

import flink.model.FlinkStreamModel;
import lombok.val;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zhangxuecheng
 * @package flink.launch.table
 * @className TableDemoApp
 * @description table demo application
 * @date 2023/6/25 11:02
 */
public class KakfaTableDemoApp extends FlinkStreamModel {
    public static void main(String[] args) throws Exception {
        val env = initEnv(args);

        val tableEnv = StreamTableEnvironment.create(env);

        sqlConnect(tableEnv);

        val kafkaTable = tableEnv.sqlQuery("select * from structTable");

        tableEnv.toDataStream(kafkaTable).print().setParallelism(1);

        env.execute("table-kafka-demo");
    }

    private static void sqlConnect(StreamTableEnvironment tableEnv) {
        // 将输入流注册为表
        tableEnv.executeSql("CREATE TABLE ODS_MES_G_QC_SN_DEFECT (\n" +
                "  after ROW<QC_LOTNO STRING, SERIAL_NUMBER STRING, QC_CNT INT, DEFECT_ID INT, DEFECT_LEVEL STRING, LOCATION STRING, DEFECT_QTY INT, ITEM_TYPE_ID STRING>) " +
                " WITH (  'connector' = 'kafka',  'topic' = 'ODS_MES_G_QC_SN_DEFECT',  " +
                "'properties.bootstrap.servers' = 'node-123:9092,node-124:9092,node-125:9092', " +
                " 'properties.group.id' = 'DwdGsnDefectApp-consumer1687948168155', " +
                " 'scan.startup.mode' = 'earliest-offset'," +
                "'json.fail-on-missing-field' = 'false', " +
                " 'format' = 'json') where DEFECT_ID =1");
    }
}