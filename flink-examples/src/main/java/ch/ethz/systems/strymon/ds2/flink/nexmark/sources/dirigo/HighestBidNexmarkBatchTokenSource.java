package ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo;

//import benchmark.dynamicFunctionRepository.dirigo.tokensource.HighestBidCollection;
//import benchmark.dynamicFunctionRepository.dirigo.tokensource.HighestBidRemoteStateCollection;
//import benchmark.dynamicFunctionRepository.dirigo.tokensource.HighestBidUserManagedCollection;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo.model.data.Bid;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo.utils.NexmarkUtils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;

import static org.apache.flink.statefun.sdk.utils.DataflowUtils.toJobFunctionType;

public class HighestBidNexmarkBatchTokenSource extends AbstractNexmarkBatchSource {
    protected final GeneratorConfig generatorConfig;
    private transient NexmarkGenerator generator;
    private volatile long numElementsEmitted;
    RuntimeContext context;
    protected short taskIndex;
    protected short jobId;
    protected Integer numSources;
    protected Integer windowSize;
    protected ArrayBlockingQueue<InternalTypedSourceObject> functionQueue;
    protected int base = 0;
    private transient boolean user = false;
    private transient boolean remoteState = false;
    protected transient long timeOffset = 0;
    protected FunctionType entryFunctionType;

    public HighestBidNexmarkBatchTokenSource(GeneratorConfig config, short id) {
        this.generatorConfig = config;
        this.jobId = id;
    }

    public HighestBidNexmarkBatchTokenSource(GeneratorConfig config, RuntimeContext context, short id) {
        this.generatorConfig = config;
        this.context = context;
        this.jobId = id;
    }

    @Override
    public void open(Configuration parameters, int parallelism, short taskIndex, ArrayBlockingQueue<InternalTypedSourceObject> functionQueue) {
        System.out.println("HighestBidMRNexmarkBatchSource open source functionQueue " + functionQueue
                + " taskIndex " + taskIndex + " shape "+this.generatorConfig.rateShape );
        if(this.generatorConfig.rateShape.equals(NexmarkUtils.RateShape.ZIPF)){
            this.generator = new NexmarkZipfGenerator(splittedConfig, taskIndex);
        }
        else{
            this.generator = new NexmarkGenerator(splittedConfig);
        }

        this.functionQueue = functionQueue;
        this.taskIndex = taskIndex;
        if (generatorConfig.params.has("parallelism")) { //milliseconds
            // read the text file from given input path
            numSources = Integer.parseInt(generatorConfig.params.get("parallelism"));
        }
        if(numSources == null){
            numSources = context.getNumberOfParallelSubtasks();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context, int parallelism, short taskIndex, ArrayBlockingQueue<InternalTypedSourceObject> functionQueue) throws Exception {
        this.functionQueue = functionQueue;
    }

    @Override
    public void snapshotState(ListState<Long> checkpointedState) throws Exception {
        checkpointedState.add(numElementsEmitted);
    }

    @Override
    public void start(SourceFunction.SourceContext<InternalTypedSourceObject> sourceContext) {
        class sourceGeneration implements Runnable {
            HighestBidNexmarkBatchTokenSource ownerSource;
            int maxParallelism;
            int parallelism;
            Long nextPriority = Long.MIN_VALUE;
            Long nextLaxity = Long.MIN_VALUE;
            public sourceGeneration(HighestBidNexmarkBatchTokenSource source){
                ownerSource = source;
                maxParallelism = source.context.getMaxNumberOfParallelSubtasks();
                parallelism = source.context.getNumberOfParallelSubtasks();
            }

            @Override
            public void run() {
                registerFunctions();
                ArrayList<Long> eventList = new ArrayList<>();
                boolean newBatch = true;
                Long currentWid = Long.MIN_VALUE;
                Long lastTS = 0L;
                int eventCount = 0;
                if(generatorConfig.params.has("window")){
                    windowSize = Integer.parseInt(generatorConfig.params.get("window"));
                }
                boolean syncTag = true;
                if(generatorConfig.params.has("sync")){
                    syncTag = Boolean.parseBoolean(generatorConfig.params.get("sync"));
                }

                System.out.println("Preparing source with window size " + windowSize + " syncTag " + syncTag + " job id " + jobId + " offset " + timeOffset + " current Time " + System.currentTimeMillis() + " baseTime " + generatorConfig.baseTime);

                try {
                    long current = System.currentTimeMillis();
                    System.out.println("Sleep " + (generatorConfig.baseTime - current) + " millis before start. tid: " + Thread.currentThread().getName());
                    //Thread.sleep(60000);
                    if(current < generatorConfig.baseTime){
                        Thread.sleep( generatorConfig.baseTime - current);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                SortedMap<Long, Bid> highestBidLocal = new TreeMap<>();

                long intervalStart = 0;
                if(taskIndex < numSources) {
                    Long cmId = null;
                    // long baseTime = generator.getCurrentConfig().baseTime;
                   long baseTime = System.currentTimeMillis();
                   generator.getCurrentConfig().resetBaseTime(baseTime);
                    Long nextEvent = null;
                    while (generator.hasNext()) {
                        if(nextEvent == null){
                            Long nextTS = generator.nextTS();
                            if(nextTS == null) break;
                            nextEvent = nextTS + baseTime;
                        }

                        eventCount++;
                        assert(generator != null);
                        assert(generator.getCurrentConfig() != null);

                        if (newBatch) {
//                                System.out.println("assign nextPriority: " + ((lastTS/ windowSize + 1) * windowSize + generatorConfig.latencyTarget)
//                                        + " task id " + taskIndex + " lastTS " + lastTS);
                            nextPriority = (lastTS!=Long.MIN_VALUE?(((lastTS - timeOffset)/ windowSize + 1) * windowSize + generatorConfig.latencyTarget ) : Long.MIN_VALUE);
                            newBatch = false;
                            intervalStart = System.currentTimeMillis();
                        }

                        lastTS = nextEvent;
                        long wid = (lastTS+ timeOffset)/windowSize;


                        if(wid> currentWid){
                            // System.out.println("Highest bid wid " + wid + " index " + taskIndex + highestBidLocal.get(currentWid));
                            currentWid = (lastTS+ timeOffset)/windowSize;
                            System.out.println("Entering new wid: " + currentWid + " task id " + taskIndex + " " + (((lastTS - timeOffset)/ windowSize + 1) * windowSize + generatorConfig.latencyTarget) + " tid: " + Thread.currentThread().getName());

                            //Remaining data batch
                            if(!eventList.isEmpty()){
                                ArrayList<Long> outputBatch = eventList;
                                FunctionInvocation invocation =  new FunctionInvocation(entryFunctionType, outputBatch, null);
                                invocation.setInternalType(entryFunctionType);
                                int workerIndex = Math.floorMod(taskIndex + base, context.getNumberOfParallelSubtasks());
                                invocation.setTranslatedOperatorIndex(maxParallelism, parallelism, workerIndex);
                                invocation.setSubIndex(taskIndex);
                                invocation.setPriority(nextPriority);
                                invocation.setLaxity(nextPriority - 60);
                                try {
                                    System.out.println("Dispatch invocation (completing window) with priority " + invocation.getPriority() + " nextPriority " + nextPriority
                                            + " last TS " + lastTS + " with offset " + (lastTS + timeOffset) + " windowSize " + windowSize + " tid: " + Thread.currentThread().getName());
                                    invocation.pTime = lastTS+ timeOffset;
                                    functionQueue.put(invocation);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                eventList = new ArrayList<>();
                                newBatch = true;
                            }

                            //Sync
                            cmId = currentWid;

                            //CM
                            FunctionInvocation invocation;
                            if(syncTag) {
                                invocation = new FunctionInvocation(entryFunctionType,  new Tuple2<Long, Long>(cmId, cmId), cmId);
                            }
                            else{
                                invocation = new FunctionInvocation(entryFunctionType,  new Tuple2<Long, Long>(cmId, cmId));
                            }
                            invocation.setInternalType(entryFunctionType);
                            int workerIndex = Math.floorMod(taskIndex + base, context.getNumberOfParallelSubtasks());
                            invocation.setTranslatedOperatorIndex(maxParallelism, parallelism, workerIndex);
                            invocation.setSubIndex(taskIndex);
                            invocation.setPriority(nextPriority);
                            invocation.setLaxity(nextPriority - 60);
                            try {
                                System.out.println("Dispatch CM with priority " + invocation.getPriority() + " nextPriority " + nextPriority
                                        + " last TS " + lastTS + " with offset " + (lastTS + timeOffset) + " windowSize " + windowSize
                                        + " tid: " + Thread.currentThread().getName());
                                invocation.pTime = lastTS+ timeOffset;
                                functionQueue.put(invocation);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            // Update priority for the message in the new window
                            nextPriority = (lastTS!=Long.MIN_VALUE?(((lastTS - timeOffset)/ windowSize + 1) * windowSize + generatorConfig.latencyTarget ) : Long.MIN_VALUE);
                        }

                        eventList.add(nextEvent);
                    //    System.out.println("TaskId " + taskIndex + " eventList size " + eventList.size() +
                    //            " batch size " + generatorConfig.getBatchSize() 
                    //         //    + " generator " + generator
                    //         //    + " next time " + (next.getEventTimestamp())
                    //            + " newBatch " + newBatch
                    //            + " nextPriority " + nextPriority + " lastTS " + lastTS
                    //            + " tid: " + Thread.currentThread().getName()
                    //            );
                        if (generator.hasNext() && (eventList.size() < generatorConfig.getBatchSize())) {
                            continue;
                        } else {
                            ArrayList<Long> outputBatch = eventList;

                            FunctionInvocation invocation =  new FunctionInvocation(entryFunctionType, outputBatch.get(outputBatch.size() - 1), null);
                            invocation.setInternalType(entryFunctionType);
                            int workerIndex = Math.floorMod(taskIndex + base, context.getNumberOfParallelSubtasks());
                            invocation.setTranslatedOperatorIndex(maxParallelism, parallelism, workerIndex);
                            invocation.setSubIndex(taskIndex);
                            if(nextPriority== Long.MIN_VALUE){
                                nextPriority = (lastTS!=Long.MIN_VALUE?(((lastTS - timeOffset)/ windowSize + 1) * windowSize + generatorConfig.latencyTarget ) : Long.MIN_VALUE);
                            }
                            invocation.setPriority(nextPriority);
                            invocation.setLaxity(nextPriority - 60);
                            long now = System.currentTimeMillis();
                            long sleep = 0;
                            if (nextEvent > now) {
                                // sleep until wall clock less than current timestamp.
                                try {
                                    sleep = nextEvent- now;
                                    Thread.sleep(sleep);

                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            System.out.println("TaskId " + taskIndex + " sleep " + (nextEvent - now)+" event timestamp " + nextEvent + " now " + now + " batch generation duration " + (System.currentTimeMillis() - intervalStart) );
                            try {
                                System.out.println("Dispatch invocation with priority " + invocation.getPriority() + " nextPriority " + nextPriority + " nextPriority " + nextPriority
                                        + " last TS " + lastTS + " with offset " + (lastTS + timeOffset) + " windowSize " + windowSize + " tid: " + Thread.currentThread().getName());
                                invocation.pTime = lastTS+ timeOffset;        
                                functionQueue.put(invocation);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            newBatch = true;
                            eventList = new ArrayList<>();
                            nextEvent = null;
                        }

                    }
                }
                System.out.println("Generator completes with eventCount " + eventCount
                        + " generator delay " + generator.getCurrentConfig().interEventDelayUs[0]
                        + " tid: " + Thread.currentThread().getName());
//                System.out.println("Source HashMap bidCount: " + highestBidLocal.keySet().stream().map(k->'{' + k.toString() + " ~ " + highestBidLocal.get(k).toString() + '}').collect(Collectors.joining(" ||| ")) +
//                         " all keys " + highestBidLocal.keySet().stream().map(k->k.toString()).collect(Collectors.joining(" ||| ")) +
//                         " keys " + Arrays.toString(highestBidLocal.keySet().toArray()));

                ownerSource.setSubIndex(taskIndex);
                int workerIndex = Math.floorMod(taskIndex + base, context.getNumberOfParallelSubtasks());
                ownerSource.setTranslatedOperatorIndex(maxParallelism, parallelism, workerIndex);
                ownerSource.setPriority(Long.MAX_VALUE);
                try {
                    functionQueue.put(ownerSource);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
        Thread thread = new Thread(new sourceGeneration(this));
        thread.start();
    }


    @Override
    public void registerFunctions()  {

        String schedulerTag = "default";
        if(generatorConfig.params.has("policy")){
            schedulerTag = generatorConfig.params.get("policy"); // "statefunstatefullbdirect"; "statefunstatefulcheckandinsert";
        }
        if(generatorConfig.params.has("window")){
            this.windowSize = Integer.parseInt(generatorConfig.params.get("window"));
        }

        if(generatorConfig.params.has("user")){
            this.user = Boolean.parseBoolean(generatorConfig.params.get("user"));
        }

        int range = 1;
        if(generatorConfig.params.has("range")){
            range = Integer.parseInt(generatorConfig.params.get("range"));
        }

        if (generatorConfig.params.has("parallelism")) { //milliseconds
            // read the text file from given input path
            numSources = Integer.parseInt(generatorConfig.params.get("parallelism"));
        }

        if (generatorConfig.params.has("remotestate")) { //milliseconds
            // read the text file from given input path
            remoteState = Boolean.parseBoolean(generatorConfig.params.get("remotestate"));
        }

        if(generatorConfig.params.has("timeoffset")){
            timeOffset = Long.parseLong(generatorConfig.params.get("timeoffset"));
        }

        int paddingSize = 0;
        if(generatorConfig.params.has("paddingsize")){
            paddingSize = Integer.parseInt(generatorConfig.params.get("paddingsize"));
        }

        if(numSources == null){
            numSources = context.getNumberOfParallelSubtasks();
        }

//        entryFunctionType = remoteState?
//                toJobFunctionType(HighestBidRemoteStateCollection.PARSE_MAP_EVENT.namespace(), (short) 1, jobId, taskIndex) :
//                (!user?
//                        toJobFunctionType(HighestBidCollection.PARSE_MAP_EVENT.namespace(), (short) 1, jobId, taskIndex):
//                        toJobFunctionType(HighestBidUserManagedCollection.PARSE_MAP_EVENT.namespace(), (short) 1, jobId, taskIndex));
//
//        try{
//            FunctionRegistration r ;
//            base = context.getNumberOfParallelSubtasks()/4* jobId; //Math.floorMod(jobId * (numSources + range + 1), context.getNumberOfParallelSubtasks());
//            // timeOffset = 5000 * jobId;
//            for (short i = 0; i < numSources ; i++){
//                r = remoteState?
//                        new FunctionRegistration(toJobFunctionType(HighestBidRemoteStateCollection.PARSE_MAP_EVENT.namespace(),(short)1, jobId, i),
//                                new HighestBidCollection.GenerateEventFunction(jobId, this.windowSize, range, generatorConfig.getBatchSize(), new Tuple2<>(taskIndex * generatorConfig.maxEvents, generatorConfig.maxEvents), base),
//                                // new HighestBidCollection.GenerateEventWithTraceFunction(jobId, this.windowSize, range, generatorConfig.getBatchSize(), new Tuple2<>(taskIndex * generatorConfig.maxEvents, generatorConfig.maxEvents)),
//                                schedulerTag, 1) :
//                        (!this.user?
//                        new FunctionRegistration(toJobFunctionType(HighestBidCollection.PARSE_MAP_EVENT.namespace(),(short)1, jobId, i),
//                                // new HighestBidCollection.GenerateEventFunction(jobId, this.windowSize, range, generatorConfig.getBatchSize(), new Tuple2<>(taskIndex * generatorConfig.maxEvents, generatorConfig.maxEvents), base),
//                                new HighestBidCollection.GenerateEventWithTraceFunction(jobId, this.windowSize, range, generatorConfig.getBatchSize(), new Tuple2<>(taskIndex * generatorConfig.maxEvents, generatorConfig.maxEvents), base),
//                                schedulerTag, 1):
//                        new FunctionRegistration(toJobFunctionType(HighestBidUserManagedCollection.PARSE_MAP_EVENT.namespace(),(short)1, jobId, i),
//                                new HighestBidUserManagedCollection.GenerateEventFunction(jobId, this.windowSize, range, generatorConfig.getBatchSize(), new Tuple2<>(taskIndex * generatorConfig.maxEvents, generatorConfig.maxEvents)),
//                                schedulerTag, 1));
//                r.setSubIndex(taskIndex);
//                r.setTranslatedOperatorIndex(context.getMaxNumberOfParallelSubtasks(), context.getNumberOfParallelSubtasks(), taskIndex);
//                r.setPriority(Long.MIN_VALUE);
//                r.setLaxity(Long.MIN_VALUE);
//                functionQueue.put(r);
//
//                r = remoteState?
//                        new FunctionRegistration(toJobFunctionType(HighestBidRemoteStateCollection.AGGREGATE_EVENT.namespace(), (short) 2, jobId, i),
//                                new HighestBidRemoteStateCollection.AggEventFunction(jobId, this.windowSize, numSources, base), // Math.floorMod(base + numSources, context.getNumberOfParallelSubtasks())),
//                                schedulerTag, numSources) :
//                        (!this.user?
//                        new FunctionRegistration(toJobFunctionType(HighestBidCollection.AGGREGATE_EVENT.namespace(), (short) 2, jobId, i),
//                               new HighestBidCollection.AggEventFunction(jobId, this.windowSize, numSources, base, timeOffset), // Math.floorMod(base + numSources, context.getNumberOfParallelSubtasks())),
//                                // new HighestBidAutoPartitionableParallelizedCollection.AggEventDuplicateStateFunction(jobId, this.windowSize, numSources, base, timeOffset, paddingSize),
//                                schedulerTag, numSources):
//                        new FunctionRegistration(toJobFunctionType(HighestBidUserManagedCollection.LOCAL_AGG_EVENT.namespace(), (short) 2, jobId, i),
//                                new HighestBidUserManagedCollection.AggEventFunction(jobId, this.windowSize, context.getNumberOfParallelSubtasks()),
//                                schedulerTag, context.getNumberOfParallelSubtasks()));
//                r.setSubIndex(taskIndex);
//                r.setTranslatedOperatorIndex(context.getMaxNumberOfParallelSubtasks(), context.getNumberOfParallelSubtasks(), taskIndex);
//                r.setPriority(Long.MIN_VALUE);
//                r.setLaxity(Long.MIN_VALUE);
//                functionQueue.put(r);
//
//
//                r = remoteState?
//                        new FunctionRegistration(toJobFunctionType(HighestBidRemoteStateCollection.AGGREGATE_EVENT.namespace(), (short) 3, jobId, i),
//                                new HighestBidRemoteStateCollection.ReductionFunction(jobId, range, base), //Math.floorMod(base + numSources + range, context.getNumberOfParallelSubtasks())),
//                                schedulerTag, range) :
//                        (!this.user? new FunctionRegistration(toJobFunctionType(HighestBidCollection.AGGREGATE_EVENT.namespace(), (short) 3, jobId, i),
//                        new HighestBidCollection.ReductionFunction(jobId, range, base), //Math.floorMod(base + numSources + range, context.getNumberOfParallelSubtasks())),
//                        schedulerTag, range):
//                        new FunctionRegistration(toJobFunctionType(HighestBidUserManagedCollection.AGGREGATE_EVENT.namespace(), (short) 3, jobId, i),
//                                new HighestBidUserManagedCollection.ReductionFunction(jobId, range),
//                                schedulerTag, range));
//                r.setSubIndex(taskIndex);
//                r.setTranslatedOperatorIndex(context.getMaxNumberOfParallelSubtasks(), context.getNumberOfParallelSubtasks(), taskIndex);
//                r.setPriority(Long.MIN_VALUE);
//                r.setLaxity(Long.MIN_VALUE);
//                functionQueue.put(r);
//
//            }
//
////            NexmarkDynamicBatchSourceFunction.sleepObject sleeper = new NexmarkDynamicBatchSourceFunction.sleepObject();
////            sleeper.setSubIndex(taskIndex);
////            sleeper.setPriority(Long.MIN_VALUE);
////            sleeper.setLaxity(Long.MIN_VALUE);
////            functionQueue.put(sleeper);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }
}
