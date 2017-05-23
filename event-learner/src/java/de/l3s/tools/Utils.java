package de.l3s.tools;

import java.io.IOException;

import org.joda.time.Months;
import org.joda.time.YearMonth;

import scala.actors.threadpool.Arrays;

import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.google.common.primitives.Ints;

/**
 * For dotgov burst detection
 * @author Admin
 *
 */
public class Utils {
	private final double densityScaling =  1.5;
	private final double gamma = 1.0 ;
	private final int states = 1;
	private final double threshold = 0.0;
	
	public static void main(String[] args) {
		Utils u = new Utils();
		//electoral_college
		int[] ts = {5, 7, 1, 1, 13, 0, 14, 3, 17, 16, 5, 6, 520, 21, 11, 10, 14, 8, 5, 5, 7, 8, 11, 7, 14, 3, 9, 9, 22, 9, 9, 6, 1, 4, 11, 19, 1, 4, 6, 2, 1, 0, 5, 2, 7, 33, 8, 1, 7, 1, 4, 4, 20, 2, 2, 12, 10, 13, 6, 1};
		double[] w = new double[ts.length];
		for (int i = 0; i < ts.length; i++) {
			w[i] = 0.0d;
		}
		System.out.println(w.length);
		YearMonth sd = new YearMonth("2008-01");
		YearMonth ed = new YearMonth("2012-12");
		Range<YearMonth> timeRange = Ranges.closed(sd, ed);
		SimpleBurstCollector<String, YearMonth> collector = u.detectTermBursts(ts, timeRange, Ints.max(ts));
		for(Burst<String, YearMonth> burst:collector.getBursts()){
			System.out.println(burst.getStart() + " : " + burst.getEnd() + " : " + burst.getStrength());
			YearMonth s = burst.getStart();
			YearMonth e = burst.getEnd();
			int i = 0;
			for (YearMonth date = sd; Months.monthsBetween(date, s).getMonths() != 0; date = date.plusMonths(1)) {
				w[i] += burst.getStrength() *  Math.pow(Math.E, (-0.4 * Math.abs(Months.monthsBetween(date, s).getMonths())));
				i++;
			}
			for (int j = 0; j < burst.getDuration().toPeriod().getMonths(); j++) {
				w[i + j] += burst.getStrength();
			}
			i = i + burst.getDuration().toPeriod().getMonths();
			for (YearMonth date = e; Months.monthsBetween(date, ed).getMonths() != 0; date = date.plusMonths(1)) {
				w[i] += burst.getStrength() *  Math.pow(Math.E, (-0.4 * Math.abs(Months.monthsBetween(date, s).getMonths())));
				i++;
			}
		}
		double max = 0;
		for (double _w : w) {
			max += _w;
		}
		int i = 0;
		for (double _w : w) {
			w[i++] = (double) _w / max;
		}
		
		System.out.println(Arrays.toString(w));
		
		
		
		
		
	
	}
	YearMonthBinner binner = new YearMonthBinner();
	public SimpleBurstCollector<String, YearMonth> detectTermBursts(int[] ts, Range<YearMonth> timeRange, int max){
		//base ts
		int[] normalize = new int[ts.length];
		System.out.println(max);
		for (int i =0; i < ts.length; i++) {
			normalize[i] = max;
		}
		SimpleBurstCollector<String, YearMonth> collector = SimpleBurstCollector
		.create(timeRange);
		BurstCollector<String, YearMonth> thresholdingCollector = ThresholdingCollector
		.create(threshold, collector);
		try {
			BurstDetector.findBursts("event", ts, normalize, states, gamma,
					densityScaling, thresholdingCollector, binner, timeRange);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return collector;

	}

}
