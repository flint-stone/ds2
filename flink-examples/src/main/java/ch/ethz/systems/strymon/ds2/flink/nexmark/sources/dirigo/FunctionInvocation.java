package ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo;

import org.apache.flink.statefun.sdk.FunctionType;

import javax.annotation.Nullable;

public class FunctionInvocation extends InternalTypedSourceObject {
    public Object messageWrapper;
    public @Nullable
    transient FunctionType functionType;
    public @Nullable transient Long ifCritical;
    public @Nullable transient Long pTime;

    public FunctionInvocation(FunctionType ft, Object message){
        super();
        functionType = ft;
        messageWrapper = message;
        ifCritical = null;
        pTime = System.currentTimeMillis();
    }

    public FunctionInvocation(FunctionType ft, Object message, Long cmId){
        super();
        functionType = ft;
        messageWrapper = message;
        ifCritical = cmId;
        pTime = System.currentTimeMillis();
    }

    @Override
    public String toString(){
        return String.format("FunctionInvocation functionType %s ifCritical %s pTime %s",
                functionType==null?"null":functionType, ifCritical==null?"null":ifCritical, pTime==null?"null":pTime);
    }
}
