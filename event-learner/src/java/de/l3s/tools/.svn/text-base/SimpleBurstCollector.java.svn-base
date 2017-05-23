package de.l3s.tools;

import java.util.Collection;
import java.util.List;

import org.joda.time.ReadablePartial;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;


public class SimpleBurstCollector<O, T extends ReadablePartial> implements
        BurstCollector<O, T> {

    private final T start;
    private final T end;
    private final List<Burst<O, T>> bursts = Lists.newArrayList();

    public SimpleBurstCollector(Range<T> dateRange) {
        this.start = dateRange.lowerEndpoint();
        this.end = dateRange.upperEndpoint();
    }

    private Burst<O, T> previousBurst = null;

    @Override
    public void collect(Burst<O, T> burst) {
        if (!(burst.getStart().equals(start) && burst.getEnd().equals(end))
                && !burst.getStart().equals(end)
                && !burst.isContainedIn(previousBurst)) {
            bursts.add(burst);
            previousBurst = burst;
        }
    }

    public static <O, U extends ReadablePartial> SimpleBurstCollector<O, U> create(
            Range<U> dateRange) {
        return new SimpleBurstCollector<O, U>(dateRange);
    }

    public Collection<Burst<O, T>> getBursts() {
        return bursts;
    }

    @Override
    public void done() {
    }


}
