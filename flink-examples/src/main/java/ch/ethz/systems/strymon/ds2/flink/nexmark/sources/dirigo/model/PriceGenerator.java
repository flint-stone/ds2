package ch.ethz.systems.strymon.ds2.flink.nexmark.sources.dirigo.model;

import java.util.Random;

/** Generates a random price. */
public class PriceGenerator {

    /** Return a random price. */
    public static long nextPrice(Random random) {
        return Math.round(Math.pow(10.0, random.nextDouble() * 6.0) * 100.0);
    }
}