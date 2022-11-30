package ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo;

import org.apache.flink.statefun.flink.core.functions.SinkObject;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;

import static org.apache.flink.statefun.flink.core.StatefulFunctionsConfig.STATFUN_SCHEDULING;

public class FunctionRegistration extends InternalTypedSourceObject {
    public FunctionType functionType;
    public StatefulFunction statefulFunction;
    public String schedulingStrategyTag;
    public Integer numUpstreams = null;
    public Integer numDownstreams = null;
    public String hostname = null;
    public Integer port = null;
    public SinkObject syncObject = null;

    public FunctionRegistration(FunctionType ft, StatefulFunction function){
        super();
        functionType = ft;
        statefulFunction = function;
        schedulingStrategyTag = STATFUN_SCHEDULING.defaultValue();
    }


    public FunctionRegistration(FunctionType ft, StatefulFunction function, String tag, Integer upstreams){
        super();
        functionType = ft;
        statefulFunction = function;
        schedulingStrategyTag = tag;
        numUpstreams = upstreams;
    }

    public FunctionRegistration(FunctionType ft, StatefulFunction function, String tag, Integer upstreams, Integer downstreams){
        super();
        functionType = ft;
        statefulFunction = function;
        schedulingStrategyTag = tag;
        numUpstreams = upstreams;
        numDownstreams = downstreams;
    }
}


