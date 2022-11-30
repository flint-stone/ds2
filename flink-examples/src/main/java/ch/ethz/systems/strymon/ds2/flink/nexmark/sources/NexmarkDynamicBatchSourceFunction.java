package ch.ethz.systems.strymon.ds2.flink.nexmark.sources;

import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo.*;
//import generator.GeneratorConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Preconditions;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class NexmarkDynamicBatchSourceFunction
        extends RichParallelSourceFunction<InternalTypedSourceObject>
        implements CheckpointedFunction, ResultTypeQueryable<InternalTypedSourceObject> {

    /** Configuration for generator to use when reading synthetic events. May be split. */
    private final GeneratorConfig generatorConfig;

    private final TypeInformation<InternalTypedSourceObject> resultType;

    private transient ListState<Long> checkpointedState;

    private transient Boolean backPressure;

    private transient Condition resumeProcessing;

    private transient Integer backpressureBufferSize;

    private transient Long sinkTime;

    private transient Long sourceTime;

    private transient int msgCounter = 0;

    private transient ReentrantLock lock;

    private static LinkedBlockingDeque<Object> functionQueue;

    private static ConcurrentMap<Integer, LinkedBlockingDeque<AbstractNexmarkBatchSource>> batchSources = new ConcurrentHashMap<>();

    private static ConcurrentMap<Integer, ArrayBlockingQueue<InternalTypedSourceObject>> functionQueues = new ConcurrentHashMap<>();

    private transient Thread tokenReceiver = null;
    public NexmarkDynamicBatchSourceFunction(GeneratorConfig config, TypeInformation<InternalTypedSourceObject> resultType) {
        this.generatorConfig = config;
        this.resultType = resultType;
    }

    public NexmarkDynamicBatchSourceFunction(GeneratorConfig config) {
        this.generatorConfig = config;
        this.resultType = null;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        functionQueues.put(getRuntimeContext().getIndexOfThisSubtask(), new ArrayBlockingQueue<>(32));
        LinkedBlockingDeque<AbstractNexmarkBatchSource> sources = new LinkedBlockingDeque<>();

        String appName = "default";
        if (generatorConfig.params.has("app")) { //milliseconds
            // read the text file from given input path
            appName = (generatorConfig.params.get("app"));
        }



        switch (appName){
            case "highestbidkeyedtwostage":
                for(short i = 0; i< generatorConfig.numDataflows; i++){
                    sources.add(new HighestBidNexmarkBatchTokenTwoStageSource(generatorConfig, getRuntimeContext(), i, true));
                }
                break;

            case "default":
                for(short i = 0; i< generatorConfig.numDataflows; i++){
                    sources.add(new HighestBidNexmarkBatchTokenTwoStageSource(generatorConfig, getRuntimeContext(), i, true));
                }
                break;
            /**
             * ************************ Archived *******************************
             */
            default:
                for(short i = 0; i< generatorConfig.numDataflows; i++){
                    sources.add(new HighestBidNexmarkBatchTokenTwoStageSource(generatorConfig, getRuntimeContext(), i, true));
                }
        }


        for(AbstractNexmarkBatchSource source : sources){
            source.splittedConfig = generatorConfig.split(getRuntimeContext().getNumberOfParallelSubtasks()).get(getRuntimeContext().getIndexOfThisSubtask());
            source.open(parameters, getRuntimeContext().getNumberOfParallelSubtasks()
                    , (short) getRuntimeContext().getIndexOfThisSubtask()
                    , functionQueues.get(getRuntimeContext().getIndexOfThisSubtask()));
        }
        batchSources.put(getRuntimeContext().getIndexOfThisSubtask(), sources);

        System.out.println("BatchSource open  " + getRuntimeContext().getNumberOfParallelSubtasks() + " " + getRuntimeContext().getIndexOfThisSubtask() + " " + batchSources);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("BatchSource initializeState  " + context);
        Preconditions.checkState(this.checkpointedState == null,
                "The " + getClass().getSimpleName() + " has already been initialized.");

        this.checkpointedState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>(
                        "elements-count-state",
                        LongSerializer.INSTANCE
                )
        );

        if (context.isRestored()) {
            List<Long> retrievedStates = new ArrayList<>();
            for (Long entry : this.checkpointedState.get()) {
                retrievedStates.add(entry);
            }

            // given that the parallelism of the function is 1, we can only have 1 state
            Preconditions.checkArgument(retrievedStates.size() == 1,
                    getClass().getSimpleName() + " retrieved invalid state.");

            for (LinkedBlockingDeque<AbstractNexmarkBatchSource> sources : batchSources.values()){
                for(NexmarkBatchSource source : sources)
                    source.initializeState(context, getRuntimeContext().getNumberOfParallelSubtasks(),
                            (short) getRuntimeContext().getIndexOfThisSubtask(),
                            functionQueues.get(getRuntimeContext().getIndexOfThisSubtask()));
            }
        }
        lock = new ReentrantLock();
        resumeProcessing = lock.newCondition();
        backPressure = false;
        backpressureBufferSize = 32;
        sinkTime = Long.MAX_VALUE;
        sourceTime = Long.MAX_VALUE;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        Preconditions.checkState(this.checkpointedState != null,
                "The " + getClass().getSimpleName() + " has not been properly initialized.");

        this.checkpointedState.clear();
        for (LinkedBlockingDeque<AbstractNexmarkBatchSource> sources : batchSources.values()) {
            for (NexmarkBatchSource source : sources){
                source.snapshotState(this.checkpointedState);
            }
        }
    }

    @Override
    public void run(SourceFunction.SourceContext<InternalTypedSourceObject> ctx) throws Exception {
        int basePort = 13579;
        if(generatorConfig.params.has("baseport")){
            basePort = Integer.parseInt(generatorConfig.params.get("baseport"));
        }

        if(generatorConfig.params.has("backpressure") && Boolean.parseBoolean(generatorConfig.params.get("backpressure"))){
            int finalBasePort = basePort;
            class BackpressureTokenReceiver implements Runnable{
                @Override
                public void run() {
                    ServerSocket externalBackpressureSocket = null;
                    Socket socket = null;
                    try {
                        int localPort = finalBasePort + getRuntimeContext().getIndexOfThisSubtask();
                        System.out.println("Try bind server socket at port " + localPort
                                + " at " + InetAddress.getLocalHost() + " tid: " + Thread.currentThread());
                        externalBackpressureSocket = new ServerSocket(localPort);
                        System.out.println("Try accepting socket connection from port " + localPort
                                + " at " + InetAddress.getLocalHost() + " tid: " + Thread.currentThread());
                        socket = externalBackpressureSocket.accept();
                        System.out.println("Accepting socket connection from " + socket.getRemoteSocketAddress().toString()
                                + " at server port " + localPort + " tid: " + Thread.currentThread());
                        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String inputToken;
                        while((inputToken = reader.readLine()) != null) {
                            // System.out.println("Receiving token " + inputToken + " tid: " + Thread.currentThread());
                            lock.lock();
                            try {
                                //backpressureBufferSize = Integer.parseInt(inputToken);
                                System.out.println("Token received " +  inputToken + " sourceTime " + sourceTime + " backPressure " + backPressure + " at tid: " + Thread.currentThread().getName());
                                sinkTime = Long.parseLong(inputToken);
                                if(backPressure && (sourceTime <= sinkTime)){
                                    System.out.println("Backpressure lifted at source receiving " +  inputToken + " current " + System.currentTimeMillis() + " id "+ getRuntimeContext().getIndexOfThisSubtask()+ " at tid: " + Thread.currentThread().getName());
                                    backPressure = false;
                                    resumeProcessing.signal();

                                }

//                                msgCounter = 0;
                            } finally {
                                lock.unlock();
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    finally {
                        try {
                            if(externalBackpressureSocket != null){
                                externalBackpressureSocket.close();
                                socket.close();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            tokenReceiver = new Thread(new BackpressureTokenReceiver());
            tokenReceiver.setName(Thread.currentThread().getName() + " (back pressure token receiver)");
            tokenReceiver.start();
        }

        for(NexmarkBatchSource source : batchSources.get(getRuntimeContext().getIndexOfThisSubtask())){
            source.start(ctx);
        }


        int sleeperCount = 0;
        System.out.println("Initialization.");
        while (true) {
            lock.lock();
            try{
                if(tokenReceiver != null && backPressure){
                    resumeProcessing.await();
                }
            }
            finally {
                lock.unlock();
            }
            InternalTypedSourceObject nextMessage = functionQueues.get(getRuntimeContext().getIndexOfThisSubtask()).take();
            //nextMessage.setSubIndex(getRuntimeContext().getIndexOfThisSubtask());
            System.out.println("Commit item " + nextMessage + " next Item");
            synchronized (ctx.getCheckpointLock()) {
                if (nextMessage instanceof NexmarkBatchSource) {
                    LinkedBlockingDeque<AbstractNexmarkBatchSource> sources = batchSources.get(nextMessage.getSubIndex());
                    sources.remove(nextMessage);
                    if (sources.size() == 0) batchSources.remove(nextMessage.getSubIndex());
                    if(nextMessage instanceof cancellationToken || (!batchSources.containsKey(nextMessage.getSubIndex()))){
                        System.out.println("close source " + getRuntimeContext().getTaskNameWithSubtasks() + " receiving nextMessage " + nextMessage);
                        Thread.sleep(Long.MAX_VALUE);
                        break;
                    }
                }
                else {
//                    try {
                        if(nextMessage instanceof  sleepObject){
                            sleeperCount++;
                            if(sleeperCount == getRuntimeContext().getNumberOfParallelSubtasks()){
			                    Thread.sleep(60000);
                            }
                            continue;
                        }
                        // System.out.println("Check backpressure  "
                        //             + " sourceTime " + (sourceTime==null?"null":sourceTime)
                        //             + " sinkTime " + (sinkTime == null?"null":sinkTime)
                        //             + " diff " + ((sourceTime!=null && sinkTime!=null) ? (sourceTime - sinkTime):"null")
                        //             + " tid: " + Thread.currentThread().getName());
                        if(tokenReceiver != null && nextMessage instanceof FunctionInvocation
                                && ((FunctionInvocation)nextMessage).ifCritical == null){
                            msgCounter++;
//                            if(msgCounter == backpressureBufferSize){
                            // System.out.println("Check backpressure functionInvocation " + ((FunctionInvocation)nextMessage).toString()
                            //         + " sourceTime " + (sourceTime==null?"null":sourceTime)
                            //         + " sinkTime " + (sinkTime == null?"null":sinkTime)
                            //         + " diff " + ((sourceTime!=null && sinkTime!=null) ? (sourceTime - sinkTime):"null")
                            //         + " tid: " + Thread.currentThread().getName());
                            if(sourceTime > sinkTime ){
                                lock.lock();
                                try{
                                    backPressure = true;
//                                    nextMessage.setRequiresAck(true);
                                    System.out.println("Backpressure triggered at source tid: " + " sourceTime " + sourceTime + " sinkTime " + sinkTime +
                                            " current " + System.currentTimeMillis() + " id " + getRuntimeContext().getIndexOfThisSubtask() + " tid: " + Thread.currentThread().getName());
                                }
                                finally {
                                    lock.unlock();
                                }
                            }
                            sourceTime = ((FunctionInvocation)nextMessage).pTime;
                        }
                        System.out.println("Commit item " + (nextMessage == null?"null":nextMessage) +
                            " id " + getRuntimeContext().getIndexOfThisSubtask() + " tid: " + Thread.currentThread().getName());
                        ctx.collect(nextMessage);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
                }
            }
        }
    }

    public static class sleepObject extends InternalTypedSourceObject {

    }

    @Override
    public void cancel() {
        try {
            functionQueues.get(getRuntimeContext().getIndexOfThisSubtask()).put(new cancellationToken());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public TypeInformation<InternalTypedSourceObject> getProducedType() {
        return resultType;
    }

    class cancellationToken extends InternalTypedSourceObject{ }
}

