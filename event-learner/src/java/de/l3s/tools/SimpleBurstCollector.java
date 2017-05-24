package de.l3s.tools;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.joda.time.ReadablePartial;

import java.util.Collection;
import java.util.List;


public class SimpleBurstCollector<O, T extends ReadablePartial> implements
        BurstCollector<O, T> {

    private final T start;
    private final T end;
    private final List<Burst<O, T>> bursts = Lists.newArrayList();
    private Burst<O, T> previousBurst = null;

    public SimpleBurstCollector(Range<T> dateRange) {
        this.start = dateRange.lowerEndpoint();
        this.end = dateRange.upperEndpoint();
    }

    public static <O, U extends ReadablePartial> SimpleBurstCollector<O, U> create(
            Range<U> dateRange) {
        return new SimpleBurstCollector<O, U>(dateRange);
    }

    @Override
    public void collect(Burst<O, T> burst) {
        if (!(burst.getStart().equals(start) && burst.getEnd().equals(end))
                && !burst.getStart().equals(end)
                && !burst.isContainedIn(previousBurst)) {
            bursts.add(burst);
            previousBurst = burst;
        }
    }

    public Collection<Burst<O, T>> getBursts() {
        return bursts;
    }

    @Override
    public void done() {
    }


}
