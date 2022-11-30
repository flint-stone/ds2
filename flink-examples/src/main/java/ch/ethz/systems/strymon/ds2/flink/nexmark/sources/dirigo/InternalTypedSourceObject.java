package ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo;

import org.apache.flink.statefun.sdk.FunctionType;

import java.io.Serializable;
import java.util.Objects;

public class InternalTypedSourceObject implements Comparable, Serializable {
    private transient Long priority = 0L;

    private transient long laxity = 0L;

    private transient final Long nano;

    private transient FunctionType internalType;

    private transient boolean requiresAck = false;

    private transient int translatedOperatorIndex;

    private transient int subIndex;

    public FunctionType getInternalType() { return internalType; }

    public void setInternalType(FunctionType internal) {
        internalType = internal;
    }

    public int getSubIndex() {
        return subIndex;
    }

    public void setSubIndex(int index) {
        subIndex = index;
    }

    public Long getPriority() {
        return priority;
    }

    public void setPriority(Long priority) {
        this.priority = priority;
    }

    public Long getLaxity() {
        return laxity;
    }

    public Long getNano() { return nano; }

    public void setLaxity(Long laxity) {
        this.laxity = laxity;
    }

    public void setTranslatedOperatorIndex(int maxParallelism, int parallelism, int taskIndex) {
        translatedOperatorIndex = taskIndex * maxParallelism / parallelism;
//                KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(
//                maxParallelism,
//                parallelism,
//                taskIndex);
    }

    public int getTranslatedOperatorIndex() {
        return translatedOperatorIndex;
    }

    public void setRequiresAck(boolean requiresAck) { this.requiresAck = requiresAck; }

    public boolean getRequiresAck() { return this.requiresAck; }

    public InternalTypedSourceObject(){
        nano = System.nanoTime();
    }

    @Override
    public int compareTo(Object o) {
        if(o == null){
            return 1;
        }
        int ret = this.priority.compareTo(Objects.requireNonNull((InternalTypedSourceObject)o).priority);
        if(ret!=0) return ret;
        ret = this.nano.compareTo(Objects.requireNonNull((InternalTypedSourceObject)o).nano);
        if(ret!=0) return ret;
        return hashCode() - o.hashCode();
    }
}
