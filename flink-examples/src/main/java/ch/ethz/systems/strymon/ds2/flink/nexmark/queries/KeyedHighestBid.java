package ch.ethz.systems.strymon.ds2.flink.nexmark.queries;

import benchmark.statefunApp.NexmarkConfiguration;
import generator.GeneratorConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
//import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.statefun.flink.core.functions.FunctionInvocation;
import org.apache.flink.statefun.flink.core.message.InternalTypedSourceObject;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import source.NexmarkDynamicBatchSourceFunction;
import utils.NexmarkUtils;
import generator.model.BidGenerator;
import model.Bid;

import javax.annotation.Nullable;
import java.util.*;

//SELECT Rstream(B.auction, B.price, B.bidder)
//        FROM Bid [RANGE 1 MINUTE SLIDE 1 MINUTE] B
//        WHERE B.price = (SELECT MAX(B1.price)
//        FROM BID [RANGE 1 MINUTE SLIDE 1 MINUTE] B1);

//CREATE TABLE discard_sink (
//        auction  BIGINT,
//        bidder  BIGINT,
//        price  BIGINT,
//        dateTime  TIMESTAMP(3),
//        extra  VARCHAR
//        ) WITH (
//        'connector' = 'blackhole'
//        );
//
//        INSERT INTO discard_sink
//        SELECT B.auction, B.price, B.bidder, B.dateTime, B.extra
//        from bid B
//        JOIN (
//        SELECT MAX(B1.price) AS maxprice, TUMBLE_ROWTIME(B1.dateTime, INTERVAL '10' SECOND) as dateTime
//        FROM bid B1
//        GROUP BY TUMBLE(B1.dateTime, INTERVAL '10' SECOND)
//        ) B1
//        ON B.price = B1.maxprice
//        WHERE B.dateTime BETWEEN B1.dateTime  - INTERVAL '10' SECOND AND B1.dateTime;


public class KeyedHighestBid {

    public static void main(String... args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        int num = 200000;
        int keys = 5;
        long delayinUs = 1000; // 1ms
        long sleep = 10L;
        int windowSize = 10000;
        int parallelism = 1;
        NexmarkUtils.RateShape shape = null;
        if (params.has("num")) {
            // read the text file from given input path
            num = Integer.parseInt(params.get("num"));
        }
        if (params.has("keys")) {
            // read the text file from given input path
            keys = Integer.parseInt(params.get("keys"));
        }
        if (params.has("delay")) {
            // read the text file from given input path
            delayinUs = Long.parseLong(params.get("delay"));
        }
        if (params.has("sleep")) { //milliseconds
            // read the text file from given input path
            sleep = Long.parseLong(params.get("sleep"));
        }
        if (params.has("shape")) { //milliseconds
            // read the text file from given input path
            switch (params.get("shape")){
                case "sine":
                    shape = NexmarkUtils.RateShape.SINE;
                    break;
                case "zipf":
                    shape = NexmarkUtils.RateShape.ZIPF;
                    break;
                default:
                    shape = NexmarkUtils.RateShape.SQUARE;
            }
        }
        if (params.has("window")) { //milliseconds
            // read the text file from given input path
            windowSize = Integer.parseInt(params.get("window"));
        }
        if (params.has("parallelism")) {
            // read the text file from given input path
            parallelism = Integer.parseInt(params.get("parallelism"));
        }
        // -----------------------------------------------------------------------------------------
        // obtain the stream execution env and create some data streams
        // -----------------------------------------------------------------------------------------

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.setString(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY, "/tmp");
        conf.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "/tmp");
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

        env.getConfig().enableSysoutLogging();
        // StatefulFunctionsConfig statefunConfig = StatefulFunctionsConfig.fromEnvironment(env);
        // statefunConfig.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);

        int maxParallelism = 1;
        System.out.print(env.getConfig());
        NexmarkConfiguration nexmarkConf = new NexmarkConfiguration(); //= NexmarkSourceOptions.convertToNexmarkConfiguration(config);
        nexmarkConf.numEventGenerators = maxParallelism;
        nexmarkConf.firstEventRate = (int) (1000000.0/(double)delayinUs); //* env.getParallelism());
        if(shape != null){
            nexmarkConf.rateShape = shape;
        }
        nexmarkConf.numEvents = num;
        GeneratorConfig generatorConfig = new GeneratorConfig(
                nexmarkConf,
                System.currentTimeMillis(),
                1,
                num,
                1);
        generatorConfig.interEventDelayUs = new double[]{delayinUs};
        generatorConfig.sleep = sleep;
        generatorConfig.params = params;

        NexmarkDynamicBatchSourceFunction sourceFunction = new NexmarkDynamicBatchSourceFunction(generatorConfig);

        DataStreamSource<InternalTypedSourceObject> bidSource = env.addSource(sourceFunction);
        int range = params.getInt("range", 1);
        int batchSize = params.getInt("batch", 1);

        DataStream<Tuple2<String, Object>> dataStream = bidSource.name("bid-source").setParallelism(params.getInt("p1", 1))
                .returns(InternalTypedSourceObject.class)
                //.rebalance()
                .filter(x->x instanceof FunctionInvocation).setParallelism(params.getInt("p1", 1))
                .flatMap(new RichFlatMapFunction<InternalTypedSourceObject, Tuple2<String, Object>>() {
                    private GeneratorConfig config;
                    private long eventId;
                    private HashMap<Long, Bid> highestBidLocal;
                    private HashMap<Long, Long> highestTS;
                    private HashMap<Long, Integer> numBidsPerGroup;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.config =  new GeneratorConfig(
                                new NexmarkConfiguration(),
                                System.currentTimeMillis(),
                                1,
                                0,
                                1);
                        int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
                        this.config = this.config.copyWith(taskIndex * generatorConfig.maxEvents, generatorConfig.maxEvents, 1);
                        this.highestBidLocal = new HashMap<>();
                        this.highestTS = new HashMap<>();
                        this.numBidsPerGroup = new HashMap<>();
                    }

                    @Override
                    public void flatMap(InternalTypedSourceObject input, Collector<Tuple2<String, Object>> collector) throws Exception {
                        FunctionInvocation invocation  = (FunctionInvocation)input;
                        Object o = invocation.messageWrapper;
                        if(o instanceof Long){
                            Long ts = (long)o;
                            HashMap<Long, ArrayList<Bid>> outputCollection = new HashMap<>();
                            for (int i = 0; i < batchSize; i++){
                                long newEventId = this.config.firstEventId + eventId;
                                Bid bid = BidGenerator.nextBid(newEventId, new Random(newEventId), ts, config);
                                eventId++;
                                long targetIndex = Math.floorMod(bid.auction, range);
                                outputCollection.putIfAbsent(targetIndex, new ArrayList<>());
                                outputCollection.get(targetIndex).add(bid);
                                // Tracing only
                                long mappedTargetIndex = targetIndex;
                                if(!highestBidLocal.containsKey(mappedTargetIndex)){
                                    highestBidLocal.put(mappedTargetIndex, bid);
                                    highestTS.put(mappedTargetIndex, bid.dateTime);
                                    numBidsPerGroup.put(mappedTargetIndex, 1);
                                }
                                else{
                                    highestBidLocal.compute(mappedTargetIndex, (k, v)->v.price>bid.price?v : bid);
                                    highestTS.compute(mappedTargetIndex, (k, v)->v>bid.dateTime?v:bid.dateTime);
                                    numBidsPerGroup.compute(mappedTargetIndex, (k, v)->v+1);
                                }
                            }

                            List<Long> targetIndexAcc = new ArrayList<>();
                            for(long targetIndex : outputCollection.keySet()){
                                long mappedTargetIndex = targetIndex;
                                targetIndexAcc.add(mappedTargetIndex);
                                System.out.println("flatmap item " + input + " output item key " + String.valueOf(mappedTargetIndex) + " value size " + outputCollection.get(targetIndex).size() + " tid: " + Thread.currentThread().getName());
                                collector.collect(new Tuple2<>(String.valueOf(mappedTargetIndex), outputCollection.get(targetIndex)));
                            }
                        }
                        else if(o instanceof Tuple2){
                            Long cmId = ((Tuple2<Long, Long>) o).f0;
                            highestBidLocal.clear();
                            highestTS.clear();
                            numBidsPerGroup.clear();
                            for(int i = 0; i < range; i++){
                                collector.collect(new Tuple2<>(String.valueOf(i), cmId));
                            }
                        }
                    }
                }).returns(new TypeHint<Tuple2<String, Object>>(){}).setParallelism(params.getInt("p1", 1));
        DataStream<Tuple2<Long, HashMap<Long, Bid>>> bidDataStream = dataStream//.keyBy(0)
                .assignTimestampsAndWatermarks(
                        new AssignerWithPunctuatedWatermarks<Tuple2<String, Object>>() {
                            Long currentHighest = 0L;
                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(Tuple2<String, Object> longObjectTuple2, long l) {
                                // System.out.println("emit watermark " + (l -1));
                                return new Watermark(l -1);
                            }

                            @Override
                            public long extractTimestamp(Tuple2<String, Object> obj, long l) {
                                //System.out.println("extractTimestamp " + obj + " l " + l);
                                if(obj.f1 instanceof ArrayList){
                                    ArrayList<Bid> bidList = (ArrayList<Bid>)obj.f1;
                                    currentHighest = Math.max(bidList.get(bidList.size() - 1).dateTime, currentHighest);
                                    System.out.println("extractTimestamp " + currentHighest);
                                    return currentHighest;
                                }
                                System.out.println("extractTimestamp " + currentHighest);
                                return currentHighest;
                            }
                        }
//                        new WatermarkStrategy<Tuple2<Long, Object>>() {
//                    @Override
//                    public WatermarkGenerator<Tuple2<Long, Object>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
//                        return new WatermarkGenerator<Tuple2<Long, Object>>() {
//                            private long currentMaxTimestamp;
//                            @Override
//                            public void onEvent(Tuple2<Long, Object> obj, long eventTimestamp, WatermarkOutput watermarkOutput) {
//                                System.out.println("Receiving input " + obj + " eventTimestamp " + eventTimestamp );
//
//                                if(obj.f0 == Long.MIN_VALUE) currentMaxTimestamp = 0L;
//                                if(obj.f1 instanceof ArrayList){
//                                    ArrayList<Bid> bidList = (ArrayList<Bid>)obj.f1;
//                                    currentMaxTimestamp = Math.max(currentMaxTimestamp, bidList.get(bidList.size() - 1).dateTime);
//                                }
//                                else if(obj.f1 instanceof Long){
//                                    currentMaxTimestamp = Math.max(currentMaxTimestamp, (Long)obj.f0 * 1000);
//                                }
//                                if(currentMaxTimestamp == Long.MIN_VALUE){
//                                    System.out.println("Emitting object with no ts " + obj );
//                                }
//                            }
//
//                            @Override
//                            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
//                                watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - 1));
//                            }
//                        };
//                    }
//                }
                ).name("flatmap-timestamp").setParallelism(params.getInt("p1", 1))
                //.countWindowAll(100)
                .keyBy(
                        new KeySelector<Tuple2<String, Object>, Object>() {
                            @Override
                            public Object getKey(Tuple2<String, Object> stringObjectTuple2) throws Exception {
                                System.out.println("keyBy item " + stringObjectTuple2);
                                return stringObjectTuple2.f0;
                            }
                        }
                )
//        keyBy(x->{
//            System.out.println("keyBy item " + x);
//            return x.f0.toString();
//        })
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .aggregate(new AggregateFunction<Tuple2<String, Object>, Tuple2<Long, HashMap<Long, Bid>>, Tuple2<Long, HashMap<Long, Bid>>>() {
                    @Override
                    public Tuple2<Long, HashMap<Long, Bid>> createAccumulator() {
                        return new Tuple2<>(0L, null);
                    }

                    @Override
                    public Tuple2<Long, HashMap<Long, Bid>> add(Tuple2<String, Object> o, Tuple2<Long, HashMap<Long, Bid>> longBidTuple2) {
                        if(o.f1 instanceof ArrayList){
                            //Bid highestBid = ((ArrayList<Bid>)o.f1).get(0);
                            HashMap<Long, Bid> accMap = longBidTuple2.f1;
                            Long highestTS = ((ArrayList<Bid>)o.f1).get(0).dateTime;
                            for(Bid bid : (ArrayList<Bid>)o.f1){
                                if(!accMap.containsKey(bid.auction)){
                                    accMap.put(bid.auction, bid);
                                }
                                else{
                                    accMap.compute(bid.auction, (k, v)->(bid.price > v.price? bid : v));
                                }
                                if(bid.dateTime > highestTS) highestTS = bid.dateTime;
                            }
                            Tuple2<Long, HashMap<Long, Bid>> ret =  new Tuple2<>(highestTS, accMap);
                            System.out.println("Current highest " + ret);
                            return ret;
                        }
                        return longBidTuple2;
                    }

                    @Override
                    public Tuple2<Long, HashMap<Long, Bid>> getResult(Tuple2<Long, HashMap<Long, Bid>> accumulator) {
                        System.out.println("getResult accumulator " + accumulator);
                        return accumulator;
                    }

                    @Override
                    public Tuple2<Long, HashMap<Long, Bid>> merge(Tuple2<Long, HashMap<Long, Bid>> acc1, Tuple2<Long, HashMap<Long, Bid>> acc2) {
                        System.out.println("merge acc1 " + acc1 + " acc2 " + acc2);
                        long highestTS = (acc1.f0>acc2.f0? acc1.f0 : acc2.f0);
                        acc2.f1.entrySet().stream().forEach(e->{
                            if(acc1.f1.containsKey(e.getKey())){
                                acc1.f1.compute(e.getKey(), (k, v)->v.price > e.getValue().price? v : e.getValue());
                            }
                            else{
                                acc1.f1.put(e.getKey(), e.getValue());
                            }
                        });
                        return new Tuple2<>(highestTS, acc1.f1);
                    }
                }).name("TumblingEventTimeWindows-Aggregate").setParallelism(params.getInt("p2", 1));
        int finalWindowSize = windowSize;
        bidDataStream.addSink(new RichSinkFunction<Tuple2<Long, HashMap<Long, Bid>>>() {
            @Override
            public void invoke(Tuple2<Long, HashMap<Long, Bid>> value, Context context) throws Exception {
                super.invoke(value, context);
                Long currentTime = System.currentTimeMillis();
                Bid top = value.f1.entrySet().stream().map(kv->(Bid)kv.getValue()).max(Comparator.comparingLong(a -> a.price)).orElse(null);
                System.out.println(String.format("Highest Bid " + top + " wid " + value.f0/ finalWindowSize
                        + " latency " + (currentTime - value.f0)));
            }
        }).name("aggregate-sink").setParallelism(params.getInt("p2", 1));;
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.execute("HighestBid");
    }

}

