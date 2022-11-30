package ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;

public interface NexmarkBatchSource extends Serializable {

    void open(Configuration parameters, int parallelism, short taskIndex, ArrayBlockingQueue<InternalTypedSourceObject> functionQueue);

    void initializeState(FunctionInitializationContext context, int parallelism, short taskIndex, ArrayBlockingQueue<InternalTypedSourceObject> functionQueue) throws Exception;

    void snapshotState(ListState<Long> checkpointedState) throws Exception;

    void start(SourceFunction.SourceContext<InternalTypedSourceObject> sourceContext);

    void registerFunctions() throws InterruptedException;
}
