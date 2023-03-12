package ch.ethz.systems.strymon.ds2.flink.nexmark.queries;

import benchmark.statefunApp.NexmarkConfiguration;
import generator.GeneratorConfig;
import generator.model.BidGenerator;
import model.Bid;
import generator.ParetoGenerator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
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

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

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


public class KeyedHighestBidCount {

    public static void main(String... args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        int num = 200000;
        if (params.has("num")) {
            // read the text file from given input path
            num = Integer.parseInt(params.get("num"));
        }

        long delayinUs = 1000; // 1ms
        if (params.has("delay")) {
            // read the text file from given input path
            delayinUs = Long.parseLong(params.get("delay"));
        }

        long sleep = 10L;
        if (params.has("sleep")) { //milliseconds
            // read the text file from given input path
            sleep = Long.parseLong(params.get("sleep"));
        }

        NexmarkUtils.RateShape shape = null;
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

        int windowSize = 10000;
        if (params.has("window")) { //milliseconds
            // read the text file from given input path
            windowSize = Integer.parseInt(params.get("window"));
        }

        Long paretoKeys = null;
        if(params.has("paretokeys")){
            paretoKeys = Long.parseLong(params.get("paretokeys"));
        }

        int bufferPerKey = 1;
        if(params.has("bufferperkey")){
            bufferPerKey = Integer.parseInt(params.get("bufferperkey"));
        }

        double paretoScale = 1.0;
        if(params.has("paretoscale")){
            paretoScale = Double.parseDouble(params.get("paretoscale"));
        }

        int keys = 1;
        if (params.has("keys")) {
            // read the text file from given input path
            keys = Integer.parseInt(params.get("keys"));
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
        int range = params.getInt("range", 1);
        int batchSize = params.getInt("batch", 1);
        nexmarkConf.batchSize = batchSize;
        GeneratorConfig generatorConfig = new GeneratorConfig(
                nexmarkConf,
                System.currentTimeMillis(),
                1,
                num,
                1);
        generatorConfig.interEventDelayUs = new double[]{delayinUs};
        generatorConfig.sleep = sleep;
        generatorConfig.params = params;
        generatorConfig.popularKeyInterval = paretoKeys;
        generatorConfig.bufferPerKey = bufferPerKey;
        generatorConfig.paretoScale = paretoScale;
        generatorConfig.numKeys = keys;

        NexmarkDynamicBatchSourceFunction sourceFunction = new NexmarkDynamicBatchSourceFunction(generatorConfig);
        DataStreamSource<InternalTypedSourceObject> bidSource = env.addSource(sourceFunction);
        int finalWindowSize = windowSize;
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
                    private HashMap<Long, ArrayList<Bid>> outputCollection;

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
                        this.outputCollection = new HashMap<>();
                        if(generatorConfig.popularKeyInterval != null) {
                            this.config.popularKeyInterval = generatorConfig.popularKeyInterval;
                            this.config.paretoGenerator = new ParetoGenerator(1, generatorConfig.paretoScale, config);
                            this.config.numKeys = generatorConfig.numKeys;
                            this.config.bufferPerKey = generatorConfig.bufferPerKey;
                        }
                        System.out.println(String.format("Initiate flatmap operator with generatorConfig %s tid: %s", generatorConfig, Thread.currentThread().getName()));
                    }

                    @Override
                    public void flatMap(InternalTypedSourceObject input, Collector<Tuple2<String, Object>> collector) throws Exception {
                        FunctionInvocation invocation  = (FunctionInvocation)input;
                        Object o = invocation.messageWrapper;
                        //System.out.println("flatmap item " + input + " tid " + Thread.currentThread().getName());
                        if(o instanceof Long){
                            Long ts = (long)o;
                            for (int i = 0; i < batchSize; i++){
                                long newEventId = this.config.firstEventId + eventId;
                                Bid bid;
                                if(config.popularKeyInterval != null){
                                    bid = BidGenerator.nextBidPareto(newEventId, new Random(newEventId), ts, config, config.numKeys);
                                }
                                else{
                                    bid = BidGenerator.nextBid(newEventId, new Random(newEventId), ts, config);
                                }
                                eventId++;
                                long key = Math.floorMod(bid.auction, config.numKeys);
                                outputCollection.putIfAbsent(key, new ArrayList<>());
                                outputCollection.get(key).add(bid);
                                // Tracing only
                                if(!highestBidLocal.containsKey(key)){
                                    highestBidLocal.put(key, bid);
                                    highestTS.put(key, bid.dateTime);
                                    numBidsPerGroup.put(key, 1);
                                }
                                else{
                                    highestBidLocal.compute(key, (k, v)->v.price>bid.price?v : bid);
                                    highestTS.compute(key, (k, v)->v>bid.dateTime?v:bid.dateTime);
                                    numBidsPerGroup.compute(key, (k, v)->v+1);
                                }
                            }
                            List<Map.Entry<Long, ArrayList<Bid>>> readyList = outputCollection.entrySet().stream().filter(e->e.getValue().size() >= config.bufferPerKey).collect(Collectors.toList());
                            for(Map.Entry<Long, ArrayList<Bid>> pair : readyList){
                                long mappedTargetIndex = Math.floorMod(pair.getKey(), range);
                                int translatedOperatorIndex = computeKeyGroupForOperatorIndex(getRuntimeContext().getMaxNumberOfParallelSubtasks(),
                                        getRuntimeContext().getNumberOfParallelSubtasks(), (int) mappedTargetIndex);
                                int i = 0;
                                int originalSize = outputCollection.get(pair.getKey()).size();
                                System.out.println("bufferPerKey " + this.config.bufferPerKey + " size " + pair.getValue().size() + " tid: " + Thread.currentThread().getName());
                                while(i + this.config.bufferPerKey < pair.getValue().size()) {
                                    collector.collect(new Tuple2<>(String.valueOf(mappedTargetIndex), new ArrayList<>(pair.getValue().subList(i, i + config.bufferPerKey))));
                                    i+= this.config.bufferPerKey;
                                }
                                outputCollection.get(pair.getKey()).subList(0, i).clear();
                                System.out.println("Dispatch regular key " + pair.getKey() + " id " + ts/finalWindowSize+ " size dispatched " + i + " pre " + originalSize + " remaining "  + outputCollection.get(pair.getKey()).size()+ " tid: " + Thread.currentThread().getName());
                            }
                            // System.out.println("Dispatch request to " + outputCollection.entrySet().stream().map(e->e.getKey()+":"+e.getValue().size()).collect(Collectors.joining("||||||")) + " id " +  ts/ finalWindowSize1 +  " tid: " + Thread.currentThread().getName());
                        }
                        else if(o instanceof Tuple2){
                            Long cmId = ((Tuple2<Long, Long>) o).f0;
                            highestBidLocal.clear();
                            highestTS.clear();
                            numBidsPerGroup.clear();
                            System.out.println("Dispatch remaining to " + outputCollection.entrySet().stream().map(e->e.getKey()+":"+e.getValue().size()).collect(Collectors.joining("||||||")) + " id " + cmId + " tid: " + Thread.currentThread().getName());
                            for(int key = 0; key < config.numKeys; key++){
                                long targetIndex = Math.floorMod(key, range); //key/((config.numKeys + range)/range);
                                ArrayList<Bid> send = new ArrayList<>();
                                int translatedOperatorIndex = computeKeyGroupForOperatorIndex(getRuntimeContext().getMaxNumberOfParallelSubtasks(),
                                        getRuntimeContext().getNumberOfParallelSubtasks(), (int)targetIndex);
                                if(outputCollection.containsKey((long)key)) send.addAll(outputCollection.get((long)key));
                                collector.collect(new Tuple2<>(String.valueOf(targetIndex), new Tuple2<>(cmId, outputCollection.containsKey((long)key)?send:null)));
                            }
                            outputCollection.clear();
                        }
                    }
                }).returns(new TypeHint<Tuple2<String, Object>>(){}).setParallelism(params.getInt("p1", 1));
        DataStream<Tuple4<Long, HashMap<Long, Bid>, Integer, String>> bidDataStream = dataStream//.keyBy(0)
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
                                    //System.out.println("extractTimestamp " + currentHighest);
                                    return currentHighest;
                                }
                                //System.out.println("extractTimestamp " + currentHighest);
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
                                //System.out.println("keyBy item " + stringObjectTuple2);
                                return stringObjectTuple2.f0;
                            }
                        }
                )
//        keyBy(x->{
//            System.out.println("keyBy item " + x);
//            return x.f0.toString();
//        })
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .aggregate(new AggregateFunction<Tuple2<String, Object>, Tuple4<Long, HashMap<Long, Bid>, Integer, String>, Tuple4<Long, HashMap<Long, Bid>, Integer, String>>() {
                    @Override
                    public Tuple4<Long, HashMap<Long, Bid>, Integer, String> createAccumulator() {
                        return new Tuple4<>(0L, null, 0, null);
                    }

                    @Override
                    public Tuple4<Long, HashMap<Long, Bid>, Integer, String> add(Tuple2<String, Object> o, Tuple4<Long, HashMap<Long, Bid>, Integer, String> acc) {
//                        if(o.f1 instanceof ArrayList){
                            //Bid highestBid = ((ArrayList<Bid>)o.f1).get(0);
                        ArrayList<Bid> inputList = null;
                        if(o.f1 instanceof ArrayList){
                            inputList = (ArrayList<Bid>) o.f1;
                            //System.out.println("Receiving regular data " + (inputList == null?"null":inputList.size()) + " key " +  Math.floorMod(inputList.get(0).auction, generatorConfig.numKeys) + " id " + o.f0 + " tid: " + Thread.currentThread().getName());
                        }
                        else if(o.f1 instanceof Tuple2){
                            inputList = ((Tuple2<Long, ArrayList<Bid>>)(o.f1)).f1;
                            long key = -1L;
                            if(inputList != null){
                                key = Math.floorMod(inputList.get(0).auction, generatorConfig.numKeys);
                            }
                            System.out.println("Receiving remaining data " + (inputList == null?"null":inputList.size()) + " key " + key + " id " + o.f0 + " tid: " + Thread.currentThread().getName());
                        }
                        if(inputList != null){
                            if(acc.f1 == null){
                                acc.f1 = new HashMap<Long, Bid>();
                            }
                            // System.out.println("Receiving item with size pre " + ((ArrayList<Bid>)o.f1).size() + " keygroup " + o.f0 + " tid: " + Thread.currentThread().getName());
                            HashMap<Long, Bid> accMap = acc.f1;
                            Long highestTS = inputList.get(0).dateTime;
                            int count = acc.f2;
                            for(Bid bid : inputList){
                                if(!accMap.containsKey(bid.auction)){
                                    accMap.put(bid.auction, bid);
                                }
                                else{
                                    accMap.compute(bid.auction, (k, v)->(bid.price > v.price? bid : v));
                                }
                                if(bid.dateTime > highestTS) highestTS = bid.dateTime;
                            }
                            count += inputList.size();
                            // System.out.println("Receiving item with size post " + ((ArrayList<Bid>)o.f1).size() + " total " + count + " keygroup " + o.f0 + " tail " + ((ArrayList<Bid>)o.f1).get(((ArrayList<Bid>)o.f1).size() - 1).dateTime + " tid: " + Thread.currentThread().getName());
                            Tuple4<Long, HashMap<Long, Bid>, Integer, String> ret =  new Tuple4<>(highestTS, accMap, count, o.f0);
                            //System.out.println("Current highest " + ret);
                            //System.out.println("Receiving regular event " + highestTS/finalWindowSize + " keygroup " + Math.floorMod(inputList.get(0).auction, generatorConfig.numKeys) + " size " + inputList.size() 
                                    // + " time " + System.currentTimeMillis() + " at tid: " + Thread.currentThread().getName());
                            return ret;
                        }
                        return acc;
                    }

                    @Override
                    public Tuple4<Long, HashMap<Long, Bid>, Integer, String> getResult(Tuple4<Long, HashMap<Long, Bid>, Integer, String> accumulator) {
                        System.out.println("getResult accumulator " + accumulator.f0 + ":" + (accumulator.f1==null?"null" : accumulator.f1.size())+ ":" + accumulator.f2 + " keygroup " + accumulator.f3+ " tid: " + Thread.currentThread().getName());
                        return accumulator;
                    }

                    @Override
                    public Tuple4<Long, HashMap<Long, Bid>, Integer, String> merge(Tuple4<Long, HashMap<Long, Bid>, Integer, String> acc1, Tuple4<Long, HashMap<Long, Bid>, Integer, String> acc2) {
                        System.out.println("merge acc1 " + acc1 + " acc2 " + acc2 + " tid: " + Thread.currentThread().getName());
                        long highestTS = (acc1.f0>acc2.f0? acc1.f0 : acc2.f0);
                        acc2.f1.entrySet().stream().forEach(e->{
                            if(acc1.f1.containsKey(e.getKey())){
                                acc1.f1.compute(e.getKey(), (k, v)->v.price > e.getValue().price? v : e.getValue());
                            }
                            else{
                                acc1.f1.put(e.getKey(), e.getValue());
                            }
                        });
                        return new Tuple4<>(highestTS, acc1.f1, acc1.f2 + acc2.f2, acc1.f3);
                    }
                }).name("TumblingEventTimeWindows-Aggregate").setParallelism(params.getInt("p2", 1));

        bidDataStream.addSink(new RichSinkFunction<Tuple4<Long, HashMap<Long, Bid>, Integer, String>>() {
            @Override
            public void invoke(Tuple4<Long, HashMap<Long, Bid>, Integer, String> value, Context context) throws Exception {
                super.invoke(value, context);
                Long currentTime = System.currentTimeMillis();
                Bid top = null;
                if (value.f1 !=null) top = value.f1.entrySet().stream().map(kv->(Bid)kv.getValue()).max(Comparator.comparingLong(a -> a.price)).orElse(null);
                System.out.println(String.format("Highest Bid " + (top==null?"null":top) + " wid " + value.f0/ finalWindowSize + " total " + value.f2 + " keygroup " + value.f3
                        + " latency " + (currentTime - value.f0)));
            }
        }).name("aggregate-sink").setParallelism(params.getInt("p2", 1));;
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setMaxParallelism(params.getInt("p1", 1));
        env.execute("HighestBid");
    }

    private static int computeKeyGroupForOperatorIndex(int maxParallelism, int parallelism, int operatorIndex) {
        return operatorIndex * maxParallelism / parallelism;
    }

}

