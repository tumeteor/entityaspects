package de.l3s.tools;

import java.io.PrintWriter;
import java.util.Locale;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.ReadablePartial;

import com.google.common.primitives.Doubles;

public class Burst<O, T extends ReadablePartial> {
    private final O object;
    private final T start;
    private final T end;
    private final double strength;
    private final int state;

    public Burst(O object, T start, T end, int state, double strength) {
        this.object = object;
        this.start = start;
        this.end = end;
        this.state = state;
        this.strength = strength;
    }

    public static <S, U extends ReadablePartial> Burst<S, U> of(S object,
            U start, U end, int state, double strength) {
        return new Burst<S, U>(object, start, end, state, strength);
    }

    public O getObject() {
        return object;
    }

    public T getStart() {
        return start;
    }

    public T getEnd() {
        return end;
    }

    public Interval getDuration() {
        return new Interval(start.toDateTime(new DateTime(0, 1, 1, 0, 0)),
                end.toDateTime(new DateTime(0, 12, 31, 23, 59)));
    }

    public int getState() {
        return state;
    }

    public double getStrength() {
        return strength;
    }

    public boolean isContainedIn(Burst<O, T> o) {
        if (o == null) {
            return false;
        }
        return object.equals(o.object) && start.equals(o.start)
                && end.equals(o.end) && state <= o.state;
    }

    @Override
    public String toString() {
        return String.format(Locale.ENGLISH, "Burst [%s, %s...%s %d@%f]", object,
                start, end, state, strength);
    }

    public void printCsv(PrintWriter out) {
        out.printf(Locale.ENGLISH, "%s;%s;%s;%d;%3.2f%n", object, start, end,
                state, strength);
    }
    
    public static void main (String[] args) {
    	double[] ts = new double[]{6.0, 12.0, 20.0, 22.0, 26.0, 28.0, 30.0, 34.0, 38.0, 41.0, 63.0, 65.0, 67.0, 69.0, 72.0, 75.0, 78.0, 81.0, 84.0, 93.0, 98.0, 106.0, 115.0, 117.0, 121.0, 124.0, 129.0, 133.0, 135.0, 137.0, 140.0, 143.0, 146.0, 149.0, 152.0, 154.0, 158.0, 160.0, 163.0, 166.0, 169.0, 173.0, 175.0};
    	System.out.println(Doubles.max(ts));
    	
    }

}
