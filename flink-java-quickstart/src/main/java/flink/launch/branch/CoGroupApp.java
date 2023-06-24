package flink.launch.branch;

import cn.hutool.core.thread.ThreadUtil;
import flink.model.FlinkBranchModel;
import flink.sink.GenericAbstractOutputFormat;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangxuecheng4441
 * @date 2022/3/15/015 13:49
 */
@Slf4j
public class CoGroupApp extends FlinkBranchModel {
    public static void main(String[] args) throws Exception {
        initEnv(args);

        //number name

        List<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1, "苹果"));
        data1.add(new Tuple2<>(2, "梨"));
        data1.add(new Tuple2<>(2, "冻梨"));
        data1.add(new Tuple2<>(3, "香蕉"));
        data1.add(new Tuple2<>(4, "石榴"));

        //number price
        List<Tuple2<Integer, Integer>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1, 7));
        data2.add(new Tuple2<>(2, 3));
        data2.add(new Tuple2<>(3, 8));
        data2.add(new Tuple2<>(4, 6));

        //number tag
        List<Tuple2<Integer, String>> data3 = new ArrayList<>();
        data3.add(new Tuple2<>(1, "7"));
        data3.add(new Tuple2<>(2, "3"));
        data3.add(new Tuple2<>(3, "8"));
        data3.add(new Tuple2<>(4, "3"));
        data3.add(new Tuple2<>(4, "4"));
        data3.add(new Tuple2<>(4, "5"));
        data3.add(new Tuple2<>(4, "6"));


        //获取数据
        val data1Source = env.fromCollection(data1)
                .setParallelism(1).name("num-string-source");

        //获取数据
        val data2Source = env.fromCollection(data2)
                .setParallelism(1).name("num-string-source");

        //获取数据
        val data3Source = env.fromCollection(data3)
                .setParallelism(1).name("num-string-source");


        data1Source.coGroup(data2Source)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .with((CoGroupFunction<Tuple2<Integer, String>,
                        Tuple2<Integer, Integer>,
                        Tuple2<Integer, List<Tuple2<String, Integer>>>>) (first, second, out) -> {
                    //所有_1相等的数据 进行coGroup
                    Integer number = 0;
                    val records = new ArrayList<Tuple2<String, Integer>>();

                    int price = 0;
                    for (Tuple2<Integer, Integer> priceTuple : second) {
                        price = priceTuple.f1;
                        number = priceTuple.f0;
                    }

                    for (Tuple2<Integer, String> nameTuple : first) {
                        records.add(Tuple2.of(nameTuple.f1, price));
                    }

                    out.collect(Tuple2.of(number, records));
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<Integer, List<Tuple2<String, Integer>>>>() {
                }))
                //输出
                .output(new GenericAbstractOutputFormat<Tuple2<Integer, List<Tuple2<String, Integer>>>>() {
                    @Override
                    public void flush(List<Tuple2<Integer, List<Tuple2<String, Integer>>>> elements) {
                        ThreadUtil.sleep(1.1 * 1000);
                        log.error("map print:{}", elements);
                    }
                })
                .setParallelism(2).name("string-source");

        env.execute("CoGroupApp");

    }


}
