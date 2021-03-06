package de.l3s.tools;

import com.google.common.collect.Range;
import org.joda.time.ReadablePartial;

import java.io.IOException;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public class BurstDetector {


    private static final int MIN_SLENGTH = 0;
    private static final double POWER_THRESH = 0;
    /* Constant parameters of the burst detection algorithm */
    //    private static final int MIN_SLENGTH = 0;
    //    private static final double POWER_THRESH = 0;
    //    private static final double HUGE_N = 1000000.0;
    private static final double DTRANS = 1.0;

    public static <O, T extends ReadablePartial> void findBursts(O object,
                                                                 int[] objectFrequencies, int[] documentFrequencies,
                                                                 int inputStates, double gamma, double densityScaling,
                                                                 BurstCollector<O, T> collector, BinningStrategy<T> binner,
                                                                 Range<T> dateRange) throws IOException {

        checkArgument(objectFrequencies.length == documentFrequencies.length,
                "#objectFrequencies (%d) != #documentFrequencies (%d)",
                objectFrequencies.length, documentFrequencies.length);
        checkNotNull(object);

        if ("".equals(object)) {
            return;
        }

        int n = documentFrequencies.length;
        Cell[] cells = computeStates(objectFrequencies, documentFrequencies, inputStates,
                gamma, densityScaling);

        for (int i = 0; i < n; i++) {
            Cell currentCell = cells[i];
            for (int level = inputStates - 1; level >= 0; level--) {
                if (isValidBurstCandidate(currentCell, level, i)) {
                    Burst<O, T> burst = toBurst(object, currentCell, level, i,
                            n, binner, dateRange);
                    collector.collect(burst);
                }
            }
        }
        collector.done();
    }

    public static <O, T extends ReadablePartial> Collection<Burst<O, T>> findBursts(
            O object, int[] objectFrequencies, int[] documentFrequencies,
            int inputStates, double gamma, double densityScaling,
            BinningStrategy<T> binner, Range<T> dateRange) throws IOException {
        SimpleBurstCollector<O, T> collector = SimpleBurstCollector
                .create(dateRange);
        findBursts(object, objectFrequencies, documentFrequencies, inputStates,
                gamma, densityScaling, collector, binner, dateRange);
        return collector.getBursts();
    }

    private static <O, T extends ReadablePartial> Burst<O, T> toBurst(O object,
                                                                      Cell cell, int level, int i, int n, BinningStrategy<T> binner,
                                                                      Range<T> dateRange) {
        T burstStart = binner.binToDate(dateRange.lowerEndpoint(), i);

        int endCell = cell.getBreakpoint(level);
        T burstEnd = endCell > 0 && endCell < n - 1 ? binner.binToDate(
                dateRange.lowerEndpoint(), endCell) : dateRange.upperEndpoint();

        double strength = cell.getTotalPower(level);

        return Burst.of(object, burstStart, burstEnd, level, strength);
    }

    private static boolean isValidBurstCandidate(Cell currentCell, int level,
                                                 int binIndex) {
        return (currentCell.getCandidate(level)
                && currentCell.getBreakpoint(level) - binIndex + 1 >= MIN_SLENGTH && currentCell
                .getTotalPower(level) >= POWER_THRESH);
    }

    /* Updated parameters */
    //private final double gamma = 1.0;         // Parameter that controls the ease with which the 
    // automaton can change states 'trans' in C code.
    //private final int inputStates = 1;        // The higher  bursting states.
    //  private final double densityScaling = 2;    // Density scaling which will affect the probability of 
    // the burst happens .

    public static Cell[] computeStates(int[] entry, int[] binBase, int burstStates,
                                       double gamma, double densityScaling) {

        double transCost = computeTransCost(entry.length, gamma);

        int levels = burstStates + 1;

        Cell[] cells = computeCosts(levels, entry, binBase, densityScaling);

        int q = computeTotals(cells, transCost, levels);
        computePathAndMark(cells, levels, q);

        int[] leftBarrier = new int[levels];

        for (int k = 0; k < levels; k++) {
            leftBarrier[k] = -1;
        }

        for (int j = 0; j < cells.length; j++) {
            Cell currentCell = cells[j];

            for (int k = 0; k < levels - 1; k++) {
                if (currentCell.getMark(k)) {
                    leftBarrier[k] = j;
                }
            }

            for (int k = 0; k < currentCell.getPath(); k++) {
                if (leftBarrier[k] >= 0) {
                    Cell barrierCell = cells[leftBarrier[k]];
                    barrierCell.setBreakpoint(k, j);
                    barrierCell.setCandidate(k, true);
                    currentCell.setEndCandidate(k, 1);
                    leftBarrier[k] = -1;
                }
            }

            for (int k = currentCell.getPath(); k < levels - 1; k++) {
                if (leftBarrier[k] >= 0) {
                    Cell barrierCell = cells[leftBarrier[k]];
                    barrierCell.addToPower(k, currentCell.getCost(k + 1)
                            - currentCell.getCost(k));
                    barrierCell.addToTotalPower(k, currentCell.getCost(levels - 1) - currentCell.getCost(k));
                }
            }
        }
        Cell lastCell = cells[cells.length - 1];

        for (int k = 0; k < levels - 1; k++) {
            if (leftBarrier[k] >= 0) {
                Cell barrierCell = cells[leftBarrier[k]];
                barrierCell.setBreakpoint(k, cells.length - 1);
                barrierCell.setCandidate(k, true);
                lastCell.setEndCandidate(k, 1);
                leftBarrier[k] = -1;
            }
        }

        for (int j = 0; j < cells.length - 1; j++) {
            Cell currentCell = cells[j];

            int p = -1;
            q = -1;
            for (int k = 0; k < levels - 1; k++) {
                if (currentCell.getCandidate(k)) {
                    p = k;
                    if (q < 0) {
                        q = k;
                    }
                }
            }
            if (p < 0) {
                continue;
            }

            currentCell.setMinRateClass(q);
            for (int k = 0; k < p; ++k) {
                if (currentCell.getCandidate(k)) {
                    /*
                     * This try to accumulate all level's weight into the lower
                     * burst level. This have created double standard of
                     * total_power value of lower level with the higher levels.
                     * Based on the paper, total_power (weight) is the rectangle
                     * area of the time period with the level.
                     * 
                     * currentCell.totalPower[p] += currentCell.power[k];
                     */
                    currentCell.setSubordinate(k, true);
                }
            }
        }

        return cells;
    }

    private static Cell[] computeCosts(int levels, int[] entry, int[] binBase,
                                       double densityScaling) {
        double expected = computeExpected(entry, binBase);

        double[] fRate = initializeFRate(expected, levels, densityScaling);

        Cell[] cells = new Cell[entry.length];

        for (int j = 0; j < cells.length; j++) {
            Cell cell = new Cell(levels);
            cells[j] = cell;
            for (int k = 0; k < levels; k++) {
                double cost = binomW(1.0 / fRate[k], entry[j], binBase[j]);
                //                logger.debug("Cost for cell {}: {}", j, cost);
                cell.setCost(k, cost);
            }
        }
        return cells;
    }

    private static int computeTotals(Cell[] cells, double transCost, int levels) {

        Cell firstCell = cells[0];
        Cell lastCell = cells[cells.length - 1];

        // first bucket
        for (int k = 0; k < levels; k++) {
            firstCell.setTotal(k, firstCell.getCost(k) + transCost * (levels - 1 - k));
        }

        for (int j = 1; j < cells.length; j++) {
            Cell currentCell = cells[j];
            Cell previousCell = cells[j - 1];
            for (int k = 0; k < levels; k++) {
                double d = currentCell.getCost(k) + previousCell.getTotal(0);
                int q = 0;
                double tmpD;
                for (int m = 1; m < levels; m++) {
                    /*
                     * The '< d' have changed to '<= d' due to we are interested
                     * on lower burst level that give the same cost. Ideally,
                     * there will not exist two levels that contains the same
                     * cost. It only happens if all costs is zero where there is
                     * not data in this bin.
                     */
                    tmpD = currentCell.getCost(k) + previousCell.getTotal(m);
                    if (m > k && (tmpD + transCost * (m - k)) <= d) {
                        d = tmpD + transCost * (m - k);
                        q = m;
                    } else if (m <= k && tmpD <= d) {
                        d = tmpD;
                        q = m;
                    }
                }
                currentCell.setTotal(k, d);
                currentCell.setPreviousPath(k, q);

            }
        }

        int q = 0;
        for (int k = 0; k < levels; k++) {
            double d = lastCell.getTotal(0);
            q = 0;
            for (int m = 1; m < levels; m++) {
                if (lastCell.getTotal(m) < d) {
                    d = lastCell.getTotal(m);
                    q = m;
                }
            }
        }
        return q;
    }

    private static void computePathAndMark(Cell[] cells, int levels, int q) {

        Cell firstCell = cells[0];
        Cell lastCell = cells[cells.length - 1];

        lastCell.setPath(q);

        for (int j = cells.length - 2; j >= 0; j--) {
            Cell nextCell = cells[j + 1];
            Cell currentCell = cells[j];
            currentCell.setPath(nextCell.getPreviousPath(nextCell.getPath()));
        }

        for (int k = firstCell.getPath(); k < levels - 1; k++) {
            firstCell.setMark(k, true);
        }

        for (int j = 1; j < cells.length; j++) {
            Cell currentCell = cells[j];
            Cell previousCell = cells[j - 1];
            for (int k = currentCell.getPath(); k < previousCell.getPath(); k++) {
                currentCell.setMark(k, true);
            }
        }
    }

    private static double[] initializeFRate(double expected, int levels,
                                            double densityScaling) {
        double[] fRate = new double[levels];

        fRate[levels - 1] = expected;

        /*
         * Change it to the same. Based on the paper. It didn't make sense to
         * have first level ratio different from other level. This sound
         * cheating.
         */
        for (int j = levels - 2; j >= 0; j--) {
            fRate[j] = fRate[j + 1] / densityScaling;
        }
        return fRate;
    }

    /**
     * Compute the TODO what is transCost
     *
     * @param n     number of buckets
     * @param gamma transition rate
     * @return the transCost
     */
    private static double computeTransCost(int n, double gamma) {
        double transCost = gamma * Math.log(n + 1) - Math.log(DTRANS);

        if (transCost < 0.0) {
            transCost = 0.0;
        }
        return transCost;
    }

    private static double computeExpected(int[] entry, int[] binBase) {
        int binN = 0;
        int binK = 0;

        for (int i = 0; i < entry.length; i++) {
            binK += entry[i];
            binN += binBase[i];
        }

        if (binN == 0 || binK == 0) {
            System.out.println(binN + " : " + binK);
            throw new RuntimeException(
                    "A word bursted on is never used");
        }

        double expected = (double) binN / (double) binK;
        return expected;
    }

    /**
     * Compute the logarithm of choose(n,k)
     */
    private static double logChoose(int n, int k) {
        int index;
        double value = 0.0;

        for (index = n; index > n - k; --index) {
            value += Math.log(index);
        }

        for (index = 1; index <= k; ++index) {
            value -= Math.log(index);
        }
        return value;
    }

    private static double binomW(double probability, int k, int n) {
        if (probability >= 1.0) {
            throw new IllegalArgumentException("probability >= 1.0, got "
                    + probability);
        }
        return -1 * (logChoose(n, k) + k * Math.log(probability) +
                (n - k) * Math.log(1.0 - probability));
    }

}
