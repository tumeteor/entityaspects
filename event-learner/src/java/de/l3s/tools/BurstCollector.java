package de.l3s.tools;

import org.joda.time.ReadablePartial;


public interface BurstCollector<O, T extends ReadablePartial> {
    public void collect(Burst<O, T> burst);

    public void done();
}
