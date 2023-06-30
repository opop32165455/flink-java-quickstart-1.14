package flink.launch.table;


import flink.model.FlinkStreamModel;
import lombok.val;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author zhangxuecheng
 * @package com.rongshu.app.dwd
 * @className DwdGQCsnDefectApp
 * @description 计算抽检不良件序号维度详情
 * @date 2023/6/28 17:08
 */
public class DwdMesGQcSnDefectApp extends FlinkStreamModel {
    public static final String G_QC_SN_DEFECT_TOPIC = "ODS_MES_G_QC_SN_DEFECT";
    public static final String G_QC_SN_DEFECT_GROUP_ID = DwdMesGQcSnDefectApp.class.getSimpleName().concat("-consumer") + System.currentTimeMillis();

    public static void main(String[] args) throws Exception {
        val env = initEnv(args);

        val tableEnvSettings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        val tEnv = StreamTableEnvironment.create(env, tableEnvSettings);
        val sql = "CREATE TABLE ODS_MES_G_QC_SN_DEFECT (\n" +
                " op_type STRING ," +
                " after ROW<QC_LOTNO STRING, " +
                "SERIAL_NUMBER STRING, " +
                "QC_CNT INT, " +
                "DEFECT_ID INT, " +
                "DEFECT_LEVEL STRING, " +
                "LOCATION STRING, " +
                "DEFECT_QTY INT, " +
                "ITEM_TYPE_ID STRING>) "
                + String.format(" WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'properties.group.id' = '%s'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'json.fail-on-missing-field' = 'false'," +
                "  'format' = 'json'" +
                ") ", G_QC_SN_DEFECT_TOPIC, "node-123:9092", G_QC_SN_DEFECT_GROUP_ID);
        tEnv.executeSql(sql);


        // 注册 Oracle 不良代码表作为维表
        String sysDefectSql = "CREATE TABLE SYS_DEFECT (\n" +
                "  DEFECT_ID INT\n" +
//                "  DEFECT_CODE STRING,\n" +
//                "  DEFECT_LEVEL STRING,\n" +
//                "  DEFECT_DESC STRING,\n" +
//                "  DEFECT_DESC2 STRING,\n" +
//                "  UPDATE_USERID INT,\n" +
//                "  UPDATE_TIME DATE,\n" +
//                "  ENABLED STRING,\n" +
//                "  DEFECT_TYPE STRING,\n" +
//                "  CODE_LEVEL STRING,\n" +
//                "  PARENT_DEFECT_ID INT,\n" +
//                "  DEFECT_TYPE_ID INT\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:oracle:thin:@oracle:1522/mes3nod',\n" +
                //"  'url' = 'jdbc:oracle:thin:@oracle:1522/MES3NODSTB',\n" +
                "  'table-name' = 'sajet.SYS_DEFECT',\n" +
                "  'driver' = 'oracle.jdbc.driver.OracleDriver',\n" +
                "  'username' = 'SAJET_READER',\n" +
                "  'truncate' = 'false',\n" +
                "  'password' = 'WAS!69*C#at'\n" +
                ")";

        tEnv.executeSql(sysDefectSql);

        val defectTable = tEnv.sqlQuery("select * from SYS_DEFECT");
        tEnv.toDataStream(defectTable)
                .print();

        val snDefectTable = tEnv.sqlQuery("select * from ODS_MES_G_QC_SN_DEFECT where op_type = 'I'");

        tEnv.toDataStream(snDefectTable, Row.class)
                .addSink(new PrintSinkFunction<>());

        env.execute(DwdMesGQcSnDefectApp.class.getSimpleName());
    }

}