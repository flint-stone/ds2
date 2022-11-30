package ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo;

//import generator.GeneratorConfig;
//import org.apache.flink.statefun.flink.core.message.InternalTypedSourceObject;
//import source.functions.NexmarkBatchSource;

public abstract class AbstractNexmarkBatchSource extends InternalTypedSourceObject implements NexmarkBatchSource {
    public GeneratorConfig splittedConfig;
}
