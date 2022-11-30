package ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo;

//import benchmark.dynamicFunctionRepository.dirigo.tokensource.HighestBidKeyedTwoStageCollection;
//import benchmark.dynamicFunctionRepository.dirigo.tokensource.HighestBidTwoStageCollection;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.statefun.sdk.FunctionType;

import static org.apache.flink.statefun.sdk.utils.DataflowUtils.toJobFunctionType;

public class HighestBidNexmarkBatchTokenTwoStageSource extends HighestBidNexmarkBatchTokenSource {
    private boolean keyBased = false;
    public HighestBidNexmarkBatchTokenTwoStageSource(GeneratorConfig config, RuntimeContext context, short id, boolean keyed) {
        super(config, context, id);
        keyBased = keyed;
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

        int range = 1;
        if(generatorConfig.params.has("range")){
            range = Integer.parseInt(generatorConfig.params.get("range"));
        }

        if (generatorConfig.params.has("parallelism")) { //milliseconds
            // read the text file from given input path
            numSources = Integer.parseInt(generatorConfig.params.get("parallelism"));
        }


        if(generatorConfig.params.has("timeoffset")){
            timeOffset = Long.parseLong(generatorConfig.params.get("timeoffset"));
        }

        if(numSources == null){
            numSources = context.getNumberOfParallelSubtasks();
        }

//        try{
//            FunctionRegistration r ;
//            base = Math.floorMod(jobId * numSources - 1, context.getNumberOfParallelSubtasks()); //context.getNumberOfParallelSubtasks()/2 * jobId; //Math.floorMod(jobId * (numSources + range + 1), context.getNumberOfParallelSubtasks());
////            r = new FunctionRegistration(toJobFunctionType(HighestBidTwoStageCollection.CONTROLLER.namespace(),(short)0, jobId, (short)0),
//            r = new FunctionRegistration(FunctionType.CONTROLLER, new HighestBidTwoStageCollection.ControllerFunction(), schedulerTag, 1);
//            r.setSubIndex(taskIndex);
//            r.setTranslatedOperatorIndex(context.getMaxNumberOfParallelSubtasks(), context.getNumberOfParallelSubtasks(), taskIndex);
//            r.setPriority(Long.MIN_VALUE);
//            r.setLaxity(Long.MIN_VALUE);
//            functionQueue.put(r);
//            if(!keyBased){
//                entryFunctionType = toJobFunctionType(HighestBidTwoStageCollection.GENERATE_EVENT.namespace(), (short) 1, jobId, taskIndex);
//                for (short i = 0; i < numSources ; i++){
//                    r = new FunctionRegistration(toJobFunctionType(HighestBidTwoStageCollection.GENERATE_EVENT.namespace(),(short)1, jobId, i),
////                                        new HighestBidAutoPartitionableCollection.GenerateEventFunction(jobId, this.windowSize, range, generatorConfig.getBatchSize(), new Tuple2<>(taskIndex * generatorConfig.maxEvents, generatorConfig.maxEvents), base),
//                            new HighestBidTwoStageCollection.GenerateEventWithTraceFunction(jobId, this.windowSize, range, generatorConfig.getBatchSize(), new Tuple2<>(taskIndex * generatorConfig.maxEvents, generatorConfig.maxEvents)),
//                            schedulerTag, 1);
//                    r.setSubIndex(taskIndex);
//                    r.setTranslatedOperatorIndex(context.getMaxNumberOfParallelSubtasks(), context.getNumberOfParallelSubtasks(), taskIndex);
//                    r.setPriority(Long.MIN_VALUE);
//                    r.setLaxity(Long.MIN_VALUE);
//                    functionQueue.put(r);
//
//                    r = new FunctionRegistration(toJobFunctionType(HighestBidTwoStageCollection.AGGREGATE_EVENT.namespace(), (short) 2, jobId, i),
//                            new HighestBidTwoStageCollection.AggEventFunction(numSources, jobId, windowSize), // Math.floorMod(base + numSources, context.getNumberOfParallelSubtasks())),
//                            schedulerTag, numSources);
//                    r.setSubIndex(taskIndex);
//                    r.setTranslatedOperatorIndex(context.getMaxNumberOfParallelSubtasks(), context.getNumberOfParallelSubtasks(), taskIndex);
//                    r.setPriority(Long.MIN_VALUE);
//                    r.setLaxity(Long.MIN_VALUE);
//                    functionQueue.put(r);
//                }
//            }
//            else{
//                entryFunctionType = toJobFunctionType(HighestBidKeyedTwoStageCollection.GENERATE_EVENT.namespace(), (short) 1, jobId, taskIndex);
//                for (short i = 0; i < numSources ; i++){
//                    r = new FunctionRegistration(toJobFunctionType(HighestBidKeyedTwoStageCollection.GENERATE_EVENT.namespace(),(short)1, jobId, i),
////                                        new HighestBidAutoPartitionableCollection.GenerateEventFunction(jobId, this.windowSize, range, generatorConfig.getBatchSize(), new Tuple2<>(taskIndex * generatorConfig.maxEvents, generatorConfig.maxEvents), base),
//                            new HighestBidKeyedTwoStageCollection.GenerateEventSingleDownstreamFunction(jobId, this.windowSize, range, generatorConfig.getBatchSize(), new Tuple2<>(taskIndex * generatorConfig.maxEvents, generatorConfig.maxEvents)),
//                            schedulerTag, 1, range);
//                    r.setSubIndex(taskIndex);
//                    r.setTranslatedOperatorIndex(context.getMaxNumberOfParallelSubtasks(), context.getNumberOfParallelSubtasks(), taskIndex);
//                    r.setPriority(Long.MIN_VALUE);
//                    r.setLaxity(Long.MIN_VALUE);
//                    functionQueue.put(r);
//
//                    r = new FunctionRegistration(toJobFunctionType(HighestBidKeyedTwoStageCollection.AGGREGATE_EVENT.namespace(), (short) 2, jobId, i),
//                            new HighestBidKeyedTwoStageCollection.AggEventFunction(numSources, jobId, windowSize), // Math.floorMod(base + numSources, context.getNumberOfParallelSubtasks())),
//                            schedulerTag, numSources);
//                    r.setSubIndex(taskIndex);
//                    r.setTranslatedOperatorIndex(context.getMaxNumberOfParallelSubtasks(), context.getNumberOfParallelSubtasks(), taskIndex);
//                    r.setPriority(Long.MIN_VALUE);
//                    r.setLaxity(Long.MIN_VALUE);
//                    functionQueue.put(r);
//                }
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
