package ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

public class UserConfiguration implements Serializable {
    /** Maximum number of events to generate. */
    public long maxEvents = 50000000;

    /**
     * Delay between events, in microseconds. If the array has more than one entry then the rate is
     * changed every {@link #stepLengthSec}, and wraps around.
     */
    public double[] interEventDelayUs;

    public int numDataflows = 1;

    public long latencyTarget = 20000;

    public long sleep = 10;

    // capture rest of the parameters
    public ParameterTool params;

}
