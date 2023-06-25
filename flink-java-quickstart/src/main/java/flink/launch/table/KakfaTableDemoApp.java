package flink.launch.table;

import flink.model.FlinkStreamModel;
import lombok.val;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
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
        tableEnv.executeSql("CREATE TABLE structTable (" +
                "  description STRING," +
                "  title STRING," +
                "  type STRING" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'mes_sajet_table_struct'," +
                "  'properties.bootstrap.servers' = 'node-123:9092,node-124:9092,node-125:9092'," +
                "  'properties.group.id' = 'test_group_0625-1420-2'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'json'" +
                ")");
    }
}