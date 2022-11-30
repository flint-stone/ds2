package ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo.model;

import java.util.Random;

/** LongGenerator. */
public class LongGenerator {

    /** Return a random long from {@code [0, n)}. */
    public static long nextLong(Random random, long n) {
        if (n < Integer.MAX_VALUE) {
            return random.nextInt((int) n);
        } else {
            // WARNING: Very skewed distribution! Bad!
            return Math.abs(random.nextLong() % n);
        }
    }
}