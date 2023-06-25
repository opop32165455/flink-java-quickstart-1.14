package flink.launch.table;

import flink.model.FlinkStreamModel;
import lombok.val;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;

import java.io.IOException;
import java.util.List;

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

        val tableEnv = StreamTableEnvironment.create(env);

        val generatorDs = FlinkStreamModel.env.addSource(new DataGeneratorSource<>(new DataGenerator<Tuple2<String, Integer>>() {
                    RandomDataGenerator generator;

                    @Override
                    public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {
                        generator = new RandomDataGenerator();
                    }

                    @Override
                    public boolean hasNext() {
                        return true;
                    }

                    @Override
                    public Tuple2<String, Integer> next() {
                        return Tuple2.of(generator.nextHexString(10), generator.nextInt(1, 50));
                    }
                }, 5L, 1000L))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
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