package ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo.utils;

import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

/** Odd's 'n Ends used throughout queries and driver. */
public class NexmarkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(NexmarkUtils.class);

    /** Mapper for (de)serializing JSON. */
    public static final ObjectMapper MAPPER = new ObjectMapper();

    /** Units for rates. */
    public enum RateUnit {
        PER_SECOND(1_000_000L),
        PER_MINUTE(60_000_000L);

        RateUnit(long usPerUnit) {
            this.usPerUnit = usPerUnit;
        }

        /** Number of microseconds per unit. */
        private final long usPerUnit;

        /** Number of microseconds between events at given rate. */
        public long rateToPeriodUs(long rate) {
            return (usPerUnit + rate / 2) / rate;
        }
    }

    /** Shape of event rate. */
    public enum RateShape {
        SQUARE,
        ZIPF,
        SINE;

        /** Number of steps used to approximate sine wave. */
        private static final int N = 10;

        /**
         * Return inter-event delay, in microseconds, for each generator to follow in order to achieve
         * {@code rate} at {@code unit} using {@code numGenerators}.
         */
        public long interEventDelayUs(int rate, RateUnit unit, int numGenerators) {
            return unit.rateToPeriodUs(rate) * numGenerators;
        }

        /**
         * Return array of successive inter-event delays, in microseconds, for each generator to follow
         * in order to achieve this shape with {@code firstRate/nextRate} at {@code unit} using {@code
         * numGenerators}.
         */
        public long[] interEventDelayUs(int firstRate, int nextRate, RateUnit unit, int numGenerators) {
            if (firstRate == nextRate) {
                long[] interEventDelayUs = new long[1];
                interEventDelayUs[0] = unit.rateToPeriodUs(firstRate) * numGenerators;
                return interEventDelayUs;
            }

            switch (this) {
                case SQUARE:
                {
                    long[] interEventDelayUs = new long[2];
                    interEventDelayUs[0] = unit.rateToPeriodUs(firstRate) * numGenerators;
                    interEventDelayUs[1] = unit.rateToPeriodUs(nextRate) * numGenerators;
                    return interEventDelayUs;
                }
                case SINE:
                {
                    double mid = (firstRate + nextRate) / 2.0;
                    double amp = (firstRate - nextRate) / 2.0; // may be -ve
                    long[] interEventDelayUs = new long[N];
                    for (int i = 0; i < N; i++) {
                        double r = (2.0 * Math.PI * i) / N;
                        double rate = mid + amp * Math.cos(r);
                        interEventDelayUs[i] = unit.rateToPeriodUs(Math.round(rate)) * numGenerators;
                    }
                    return interEventDelayUs;
                }
            }
            throw new RuntimeException(); // switch should be exhaustive
        }

        /**
         * Return delay between steps, in seconds, for result of {@link #interEventDelayUs}, so as to
         * cycle through the entire sequence every {@code ratePeriodSec}.
         */
        public int stepLengthSec(int ratePeriodSec) {
            int n = 0;
            switch (this) {
                case SQUARE:
                    n = 2;
                    break;
                case SINE:
                    n = N;
                    break;
                case ZIPF:
                    n = N;
                    break;
            }
            return (ratePeriodSec + n - 1) / n;
        }
    }

    //https://stackoverflow.com/questions/27105677/zipfs-law-in-java-for-text-generation-too-slow
    public static class FastZipfGenerator
    {
        private Random random = new Random(0);
        private NavigableMap<Double, Integer> map;
        private final int min;
        private final int size;
//        private final int binSize;
        private final ParetoDistribution distribution;

        public FastZipfGenerator(double skew, int size, double mean)
        {
            this.min = (int) (mean*skew);
            double alpha = (mean) / (mean - min);
            this.size = 16384;
            map = computeMap(size, alpha);
            distribution = new ParetoDistribution(this.min, alpha);
            System.out.println("Initializing zipf generator " + " min " + this.min +
                    " alpha " + alpha + " distribution mean " + distribution.getNumericalMean());
        }

        private NavigableMap<Double, Integer> computeMap(
                int size, double alpha)
        {
            NavigableMap<Double, Integer> map =
                    new TreeMap<Double, Integer>();

            for(int i = 0; i <= this.size ; i++){
                map.put(1-Math.pow(this.min/(double)(this.min + i), alpha), this.min + i);
            }
            return map;
        }

        public int next()
        {
            int ret = (int)(distribution.sample()) + 1;
            return ret;
        }

    }

}
