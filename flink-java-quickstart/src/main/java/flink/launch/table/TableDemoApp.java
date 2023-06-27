package flink.launch.table;

import flink.model.FlinkStreamModel;
import flink.source.TestDataGeneratorSource;
import lombok.val;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Date;

/**
 * @author zhangxuecheng
 * @package flink.launch.table
 * @className TableDemoApp
 * @description table demo application
 * @date 2023/6/25 11:02
 */
public class TableDemoApp extends FlinkStreamModel {
    public static void main(String[] args) throws Exception {
        val env = initEnv(args);

        val tableEnvSettings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        val tableEnv = StreamTableEnvironment.create(env,tableEnvSettings);

        val generatorDs = FlinkStreamModel.env.addSource(new TestDataGeneratorSource(5,1000))
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, Integer, Date>>() {
                }));

        //generatorDs.print();


        tableEnv.createTemporaryView("generator_table", generatorDs, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.INT())
                .build());

        val table = tableEnv.sqlQuery("select * from generator_table");

        tableEnv.toDataStream(table).print().setParallelism(1);

        env.execute("table-demo");
    }
}