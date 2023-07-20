package flink.launch.stream.broadcast;

import cn.hutool.core.map.MapUtil;
import flink.model.FlinkStreamModel;
import flink.source.DateGenUtil;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author zhangxuecheng
 * @package flink.launch.stream.broadcast
 * @className BroadcastStateDemoApp
 * @description BroadcastState Demo
 * @date 2023/7/10 10:17
 */
@Slf4j
public class BroadcastStateDemoApp extends FlinkStreamModel {
    public static final String BROADCAST = "Broadcast-Stream";

    public static void main(String[] args) throws Exception {
        var env = initEnv(args);
        val generatorDsV1 = env.addSource(DateGenUtil.getDataSource(2, 1000, new DateGenUtil.TSupplier<Tuple2<Integer, String>>() {
                            @Override
                            public Tuple2<Integer, String> get() {
                                return Tuple2.of(DateGenUtil.randomDataGenerator.nextInt(1, 4), DateGenUtil.randomDataGenerator.nextHexString(10));
                            }
                        })
                )
                .returns(TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {
                }))
                .setParallelism(1);


        val generatorDsV2 = env.addSource(DateGenUtil.whileSource(20 * 1000, (DateGenUtil.TConsumer<SourceFunction.SourceContext<Tuple2<Integer, String>>>) sourceContext ->
                                {
                                    Map<Integer, String> config = MapUtil.ofEntries(
                                            MapUtil.entry(1, "c-1"),
                                            MapUtil.entry(2, "c-2"),
                                            MapUtil.entry(3, "c-3"),
                                            MapUtil.entry(4, "c-4")
                                    );
                                    config.forEach((k, v) -> {
                                        sourceContext.collect(Tuple2.of(k, v));
                                    });
                                }
                        )
                )
                .returns(TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {
                }))
                .setParallelism(1);

        //广播到map
        val erpOrderMapping = new MapStateDescriptor<>(BROADCAST, TypeInformation.of(Integer.class), TypeInformation.of(String.class));
        val broadcastStream = generatorDsV2.broadcast(erpOrderMapping);

        generatorDsV1.keyBy(t -> t.f0)
                .connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    private MapState<Integer, String> mapState;
                    private MapStateDescriptor<Integer, String> mapStateDescriptor;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mapStateDescriptor = new MapStateDescriptor<>(BROADCAST, Integer.class, String.class);
                        // 初始化
                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);

                    }

                    @Override
                    public void processElement(Tuple2<Integer, String> value, KeyedBroadcastProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>.ReadOnlyContext ctx, Collector<Tuple3<Integer, String, String>> out) throws Exception {
                        val broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                        Integer currentKey = ctx.getCurrentKey();
                        log.info("key: {} value:{}", currentKey, value);

                        broadcastState.immutableEntries().forEach(t -> {
                            log.info("broadcastState key: {} value:{}", t.getKey(), t.getValue());
                        });

                    }

                    /**
                     *
                     * @param value todo The stream element. 连接的广播流 数据入口
                     * @param ctx A {@link Context}
                     * @param out The collector to emit resulting elements to
                     * @throws Exception Exception
                     */
                    @Override
                    public void processBroadcastElement(Tuple2<Integer, String> value, KeyedBroadcastProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>.Context ctx, Collector<Tuple3<Integer, String, String>> out) throws Exception {
                        //value 所有key的taskManager都会执行 获取相同的广播数据
                        log.info("processBroadcastElement  value:{}", value);

                        //todo keyed process 使用非 keyed mapState不正确 我的理想情况是 在keyProcess 分成n份到每个taskManger时 能拿到对应MapState 而不是广播的大state

                        //todo Context中 维护的一个 空的广播状态 注意 广播流和广播状态是两个联系不大的东西
                        ctx.getBroadcastState(mapStateDescriptor).put(value.f0, value.f1);
//                        ctx.applyToKeyedState(mapStateDescriptor, new KeyedStateFunction<Integer, MapState<Integer, String>>() {
//                            @Override
//                            public void process(Integer key, MapState<Integer, String> state) throws Exception {
//                                log.info("applyToKeyedState key: {} value:{}", key, value);
//                                state.entries().forEach((t -> {
//                                    log.info("applyToKeyedState MapState key: {} value:{}", t.getKey(), t.getValue());
//                                }));
//                            }
//                        });


                    }
                })
                .setParallelism(4)
                .print();

        env.execute();
    }
}