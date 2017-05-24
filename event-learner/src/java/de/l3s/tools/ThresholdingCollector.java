package de.l3s.tools;

import org.joda.time.ReadablePartial;

/**
 * A BurstCollector that passes only burst above a certain strength to an inner
 * collector.
 *
 * @author Gerhard Gossen <gossen@l3s.de>
 */
public class ThresholdingCollector<O, T extends ReadablePartial> implements
        BurstCollector<O, T> {

    private final double threshold;
    private final BurstCollector<O, T> inner;

    public ThresholdingCollector(double threshold, BurstCollector<O, T> inner) {
        this.threshold = threshold;
        this.inner = inner;
    }

    public static <O, T extends ReadablePartial> ThresholdingCollector<O, T> create(
            double threshold, BurstCollector<O, T> inner) {
        return new ThresholdingCollector<O, T>(threshold, inner);
    }

    @Override
    public void collect(Burst<O, T> burst) {

        if (burst.getStrength() >= threshold) {
            inner.collect(burst);
        }
    }

    @Override
    public void done() {
    }

}
