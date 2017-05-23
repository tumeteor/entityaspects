package l3s.de.event.wiki;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import l3s.de.event.wiki.similarity.PageSimilarity;
import l3s.de.rjava.TextConsole;



import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.ArrayUtils;
import org.jfree.data.time.Year;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.Period;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.google.common.primitives.Ints;


import de.l3s.common.QuickSort;
import de.l3s.tools.Burst;
import de.l3s.tools.BurstCollector;
import de.l3s.tools.BurstDetector;
import de.l3s.tools.LocalDateBinner;
import de.l3s.tools.SimpleBurstCollector;
import de.l3s.tools.ThresholdingCollector;


import au.com.bytecode.opencsv.CSVReader;

public class RWikiEvents {
	public Rengine engine;
	LocalDateBinner binner = new LocalDateBinner();
	private final double densityScaling =  1.5;
	private final double gamma = 1.0 ;
	private final int states = 1;
	private final double threshold = 0.0;


	public RWikiEvents() {
		//System.setProperty("R_HOME", "/usr/bin/R");

//		String[] rOptions = { "--vanilla" };
//		engine = new Rengine(rOptions, false, new TextConsole());
//		engine.eval("library(e1071)");
//		engine.eval("library(lsa)");	
//		engine.eval("library(TTR)");
		//engine.eval("library(bursts)");
		//engine.eval("library(epiR)");

		//engine.eval("library(TTR), lib.loc=\"C:/Users/Kanhabua/Documents/R/win-library/3.0\"");
		//engine.eval("library(e1071), lib.loc=\"C:/Users/Kanhabua/Documents/R/win-library/3.0\"");
		//engine.eval("library(lsa), lib.loc=\"C:/Users/Kanhabua/Documents/R/win-library/3.0\"");

	}

	public void define_ccf() {
		engine.eval("ccf.tab <-  function (x, y, lag.max = NULL, type = c(\"correlation\", \"covariance\"), " +
				"plot = TRUE, na.action = na.fail, ...)" +
				"{" +
				"type <- match.arg(type) +" +
				"if (is.matrix(x) || is.matrix(y)) stop(\"univariate time series only\")" +
				"X <- ts.intersect(as.ts(x), as.ts(y))" +
				"colnames(X) <- c(deparse(substitute(x))[1L], deparse(substitute(y))[1L])" +
				"acf.out <- acf(X, lag.max = lag.max, plot = FALSE, type = type, na.action = na.action)" +
				"lag <- c(rev(acf.out$lag[-1, 2, 1]), acf.out$lag[, 1, 2])" +
				"y <- c(rev(acf.out$acf[-1, 2, 1]), acf.out$acf[, 1, 2])" +
				"acf.out$acf <- array(y, dim = c(length(y), 1L, 1L))" +
				"acf.out$lag <- array(lag, dim = c(length(y), 1L, 1L))" +
				"acf.out$snames <- paste(acf.out$snames, collapse = \" & \")" +
				"if (plot) {" +
				"plot(acf.out, ...)}" +
				" acd <- data.frame(cbind(lag=acf.out$lag,acf=acf.out$acf))	" +
				"a <- as.matrix(acd) 							"
		);
	}

	public void init() {
		System.out.println("***begin init()");


		engine.eval("all_div <- read.csv(\"/home/kanhabua/projects/deduplication/data/WikiEvents/matrix/all_events_en_transposed.txt\", header=T, sep=\"\t\")");
		engine.eval("source('/home/tunguyen/correlation/ccf.r')");

		System.out.println("***end init()");
	}

	/**
	 * 
	 * @param st is starting time
	 * @param et is ending time
	 * @param sc is the column number of starting event
	 * @param id is the id of ccf
	 * @param eid is the idx of the main event (among the set of events)
	 * @return
	 */
	public void ccf(int st, int et, int sc1, int sc2, int id, int eid, int lag, double sm) {
		System.out.println(st + " : " + et + " : " + sc1+ " : " + sc2);
		//System.out.println(st2 + " : " + et2 + " : " + sc2);
		engine.eval("wp1" + eid + " <- all_div[" + st + ":" + et + ", c(" + sc1 + ")]");
		engine.eval("wp1_ts" + eid + " <- ts(wp1" + eid + ")");
		engine.eval("wp1_ts_sm" + eid + " <- SMA(wp1_ts" + eid + ", " + sm + ")");
		engine.eval("wp2" + eid + " <- all_div[" + st + ":" + et + ", c(" + sc2 + ")]");
		engine.eval("wp2_ts" + eid + " <- ts(wp2" + eid + ")");
		engine.eval("wp2_ts_sm" + eid + " <- SMA(wp2_ts" + eid + ", " + sm + ")");
		engine.eval("cv_wp" + eid + "_" + id + " = ccf.tab(wp1_ts_sm" + eid + ", wp2_ts_sm" + eid + ", lag.max=" + lag + ", plot = FALSE, ylab = \"cross-correlation\", na.action=na.pass)");

		System.out.println("\twp1" + eid + " <- all_div[" + st + ":" + et + ", c(" + sc1 + ")]");
		System.out.println("\twp1_ts" + eid + " <- ts(wp1" + eid + ")");
		System.out.println("\twp1_ts_sm" + eid + " <- SMA(wp1_ts" + eid + ", " + sm + ")");
		System.out.println("\twp2" + eid + " <- all_div[" + st + ":" + et + ", c(" + sc2 + ")]");
		System.out.println("\twp2_ts" + eid + " <- ts(wp2" + eid + ")");
		System.out.println("\twp2_ts_sm" + eid + " <- SMA(wp2_ts" + eid + ", " + sm + ")");
		System.out.println("\tcv_wp" + eid + "_" + id + " = ccf.tab(wp1_ts_sm" + eid + ", wp2_ts_sm" + eid + ", lag.max=" + lag + ", plot = FALSE, ylab = \"cross-correlation\", na.action=na.pass)");		
	}

	/**
	 * 
	 * @param st is starting time
	 * @param et is ending time
	 * @param sc is the column number of starting event
	 * @param id is the id of ccf
	 * @param eid is the idx of the main event (among the set of events)
	 * @return
	 */
	public void kurtosis_sse(int st, int et, int sc, int id, int eid) {
		engine.eval("wp" + eid + " <- all_div[" + st + ":" + et + ", c(" + sc + ")]");
		System.out.println("wp" + eid + " <- all_div[" + st + ":" + et + ", c(" + sc + ")]");
		engine.eval("wp_ts" + eid + " <- ts(wp" + eid + ")");
		System.out.println("wp_ts" + eid + " <- ts(wp" + eid + ")");
		REXP x = engine.eval("wp_ts" + eid);
		System.out.println(Arrays.toString(x.asStringArray()));
		engine.eval("kv_wp" + eid + "_" + id + " = kurtosis(wp_ts" + eid + ")");
		System.out.println("kv_wp" + eid + "_" + id + " = kurtosis(wp_ts" + eid + ")");
		engine.eval("sv_wp" + eid + "_" + id + " = HoltWinters(wp_ts" + eid + ", gamma=FALSE)");
	}


	/**
	 * 
	 * @param st is starting time
	 * @param et is ending time
	 * @param sc is the column number of starting event
	 * @param id is the id of ccf
	 * @param eid is the idx of the main event (among the set of events)
	 * @return burst level
	 */
	public double[] getBurstLevels(int st, int et, int sc) {
		System.out.println(st + " : " + et + " : " + sc);
		engine.eval("wp <- all_div[" + st + ":" + et + ", c(" + sc + ")]");
		engine.eval("wp <- as.factor(wp)");
		engine.eval("offset <- epi.offset(wp)");
		engine.eval("offset <- unique (offset)");
		engine.eval("bursts <- kleinberg(offset)");
		REXP x = engine.eval("bursts$level");
		/*
		REXP x = engine.eval("all_div[" + st + ":" + et + ", c(" + sc + ")]");
		double[] x1 = x.asDoubleArray();
		System.out.println(x1.length);
		//remove adjacent duplicate 
		LinkedList<Double> x2 = new LinkedList<Double>();
		for (double x1_ : x1) {
			if (x2.size() == 0 || x2.peek() != x1_)
				x2.push(x1_);
		}
		Iterator<Double> itr = x2.descendingIterator();
		double[] x3 = new double[x2.size()];
		int idx = 0;
		while(itr.hasNext()) {
			x3[idx++] = itr.next();
		}
		engine.assign("wp", x3);
	    engine.eval("bursts <- kleinberg(wp)");
	    REXP xx = engine.eval("bursts$level");
		 */
		return x.asDoubleArray();

	}

	/**
	 * 
	 * @param st
	 * @param et
	 * @param sc
	 * @return
	 */
	public double[] getLocalMaxima(int st, int et, int sc) {
		engine.eval("wp <- all_div[" + st + ":" + et + ", c(" + sc + ")]");
		REXP x = engine.eval("which(diff(sign(diff(wp)))==-2)+1");
		System.out.println("1: " + Arrays.toString(x.asDoubleArray()));
		return x.asDoubleArray();		
	}

	public double[] getMaxMin(int st, int et, int sc) {
		engine.eval("wp <- all_div[" + st + ":" + et + ", c(" + sc + ")]");
		REXP x = engine.eval("max(wp)");
		REXP y = engine.eval("min(wp)");
		REXP z = engine.eval("mean(wp)");

		return new double[] {x.asInt(), y.asInt(), z.asDouble()};

	}

	public String getBurstLevels_(LocalDate sd, LocalDate ed, int st, int et, int sc) {
		System.out.println(st + " : " + et + " : " + sc);
		REXP x = engine.eval("all_div[" + st + ":" + et + ", c(" + sc + ")]");

		//can only extract as double array even though its int array
		double[] ts_ = x.asDoubleArray();
		int[] ts = new int[ts_.length];
		for (int i = 0; i < ts.length; i++) {
			ts[i] = (int) ts_[i];
		}
		int max = Ints.max(ts);
		if (max == 0) return "N/A";
		Range<LocalDate> timeRange = Ranges.closed(sd, ed);
		SimpleBurstCollector<String,LocalDate> collector = detectTermBursts(ts, timeRange, max);

		StringBuffer sb = new StringBuffer();
		for(Burst<String, LocalDate> burst:collector.getBursts()){
			sb.append(burst.getStrength()).append(":").append(burst.getDuration().toPeriod().getDays());
			sb.append("\t");
		}
		return sb.toString();
	}

	public SimpleBurstCollector<String, LocalDate> detectTermBursts(int[] ts, Range<LocalDate> timeRange, int max){
		//base ts
		int[] normalize = new int[ts.length];
		System.out.println(max);
		for (int i =0; i < ts.length; i++) {
			normalize[i] = max;
		}
		SimpleBurstCollector<String, LocalDate> collector = SimpleBurstCollector
		.create(timeRange);
		BurstCollector<String, LocalDate> thresholdingCollector = ThresholdingCollector
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
	
	public int[] getTimeSeries(int st, int et, int sc){
		REXP x = engine.eval("all_div[" + st + ":" + et + ", c(" + sc + ")]");

		//can only extract as double array even though its int array
		double[] ts_ = x.asDoubleArray();
		int[] ts = new int[ts_.length];
		for (int i = 0; i < ts.length; i++) {
			ts[i] = (int) ts_[i];
		}
		return ts;
	}

	/**
	 * 
	 * @param sd event starting date 
	 * @param ed event ending date
	 * @param st 
	 * @param et
	 * @param sc
	 * @return
	 */
	public double filterByBursts(LocalDate sd, LocalDate ed, int st, int et, int sc) {
		LocalDate start = sd.minusMonths(1);
		LocalDate end = ed.plusMonths(1);
		//just to make sure the number of days plus or minus ~30
		st = (st < Days.daysBetween(sd, start).getDays()) ? 0 : st - Days.daysBetween(sd, start).getDays();
		et = et + Days.daysBetween(ed, end).getDays();
		//one month before and one month after the event
		Interval eventP = new Interval(start.toDateTime(new DateTime(0, 1, 1, 0, 0)),
				end.toDateTime(new DateTime(0, 12, 31, 23, 59)));

		int[] ts = getTimeSeries(st, et, sc);
		int max = Ints.max(ts);
		if (max == 0) return 0.0d;
		Range<LocalDate> timeRange = Ranges.closed(sd, ed);
		
		SimpleBurstCollector<String,LocalDate> collector = null;
		try {
			collector = detectTermBursts(ts, timeRange, max);
		} catch (java.lang.IllegalArgumentException iae) {}
		if (collector != null) {
			int i = 0;
			for(Burst<String, LocalDate> burst:collector.getBursts()){
				i = burst.getDuration().overlaps(eventP) ? i + 1 : i;
			}
			return (double) i / collector.getBursts().size();
		} else {
			return 0.0d;
		}		
	}
	
	public static void main (String[] args) {
	    RWikiEvents r = new RWikiEvents();
		int[] ts = new int[]{416, 433, 435, 228, 341, 312, 423, 430, 384, 418, 322, 223, 379, 429, 21064, 10749, 8937, 4810, 3385, 3370, 3463, 3888, 2647, 2546, 1697, 1535, 2048, 2435, 1971};
		int max = 21064;
		LocalDate start = r.binner.parseDate("2011-02-22").minusDays(14);
		LocalDate end = start.plusDays(ts.length);
		Range<LocalDate> timeRange = Ranges.closed(start, end);
		SimpleBurstCollector<String,LocalDate> collector = null;
		try {
		collector = r.detectTermBursts(ts, timeRange, max);
		} catch (java.lang.IllegalArgumentException iae) {
			
		}
		if (collector != null) {
			StringBuffer sb = new StringBuffer();
			for(Burst<String, LocalDate> burst:collector.getBursts()){
				sb.append(burst.getStrength()).append(":").append(burst.getDuration().toPeriod().getDays());
				sb.append("\t");

				System.out.println(burst.getStart() + " : " + burst.getEnd());
			}
			System.out.println(sb.toString());
		}
	}

	/**
	 * Load event metadata from csv file
	 * sort by starting date (ascending)
	 * @param metadataFilePath
	 * @return
	 */
	public TreeMap<String, EventPeriod>  getEventMetadata(String metadataFilePath) {
		HashMap<String, EventPeriod> expr_map = new HashMap<String, EventPeriod>();
		CSVReader reader;
		try {
			reader = new CSVReader(new InputStreamReader(getClass().getResourceAsStream(metadataFilePath)), '\t');
			String[] entry;
			while ((entry = reader.readNext()) != null) {
				expr_map.put(entry[0], new EventPeriod(binner.parseDate(entry[1]),
						binner.parseDate(entry[2])));
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//sorting by event starting date
		MapValueComparator bvc =  new MapValueComparator(expr_map);
		TreeMap<String, EventPeriod> sorted_map = new TreeMap<String, EventPeriod>(bvc);
		sorted_map.putAll(expr_map);

		return sorted_map;

	}

	/**
	 * Load event metadata from csv file
	 * no sort
	 * @param metadataFilePath
	 * @return
	 */
	public HashMap<String, EventPeriod>  getEventMetadata_(String metadataFilePath) {
		HashMap<String, EventPeriod> expr_map = new HashMap<String, EventPeriod>();
		CSVReader reader;
		try {
			reader = new CSVReader(new InputStreamReader(getClass().getResourceAsStream(metadataFilePath)), '\t');
			String[] entry;
			while ((entry = reader.readNext()) != null) {
				expr_map.put(entry[0], new EventPeriod(binner.parseDate(entry[1]),
						binner.parseDate(entry[2])));
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return expr_map;

	}

	class EventPeriod {
		public LocalDate startingDate;
		public LocalDate endingDate;

		public EventPeriod(LocalDate startingDate, LocalDate endingDate) {
			this.startingDate = startingDate;
			this.endingDate = endingDate;
		}
	}

	class MapValueComparator implements Comparator<String> {

		Map<String, EventPeriod> base;
		public MapValueComparator(HashMap<String, EventPeriod> expr_map) {
			this.base = expr_map;
		}   
		public int compare(String a, String b) {
			return base.get(a).startingDate.compareTo(base.get(b).startingDate);
		}
	}



	public static void main__(String[] args) throws ParseException, IOException {
		RWikiEvents r = new RWikiEvents();
		r.init();
		System.out.println("***1");

		// create the command line parser
		CommandLineParser parser = new GnuParser();

		// create Options object
		Options options = new Options();

		options.addOption( "c", "category", true, "is the event category e.g., atlantic_hurricane_season" );
		options.addOption( "cf", "category-meta", true, "is the path to metadata event category file, contains event dates e.g., /atlantic_hurricane.csv" );
		options.addOption( "d", "day", true, "is the number of days used for sliding windows" );
		options.addOption( "l", "lag", true, "is the time lag used in cross-correlation" );
		options.addOption( "t", "type", true, "is the event type, e.g., u" );
		options.addOption( "sm", "smoothing", true, "smoothing parameter" );

		System.out.println("***2");

		// parse the command line arguments
		CommandLine line = parser.parse( options, args);
		//event type e.g., u
		String type = line.hasOption("t") ? line.getOptionValue("t") : "u";
		//event category
		String category = line.hasOption("c") ? line.getOptionValue("c") : "atlantic_hurricane_season";
		//path to event metadata file contains event date (csv)
		String c_path = line.hasOption("cf") ? line.getOptionValue("cf") : "/atlantic_hurricane.csv";
		//lag
		String l_ = line.hasOption("l") ? line.getOptionValue("l") : "7";
		int l = Integer.parseInt(l_);  
		//sm
		String sm_ = line.hasOption("sm") ? line.getOptionValue("sm") : "1";
		double sm = Double.parseDouble(sm_);

		//sliding window
		String d_ = line.hasOption("d") ? line.getOptionValue("d") : "14";
		int d = Integer.parseInt(d_);

		System.out.println("***3");

		//load event metatdata from csv file
		HashMap<String, EventPeriod> event_map = r.getEventMetadata_(c_path);
		Set<String> keyset = event_map.keySet();

		//triggering event list (after 2008)
		ArrayList<String> trg_events = new ArrayList<String>();
		LocalDate starting_point = r.binner.parseDate("2008-01-01");
		for(String expr : keyset){
			if(event_map.get(expr).startingDate.isAfter(starting_point) && !trg_events.contains(expr)) 
				trg_events.add(expr);
		}
		QuickSort.sort(trg_events);

		System.out.println("***4");

		ArrayList<String> event_idx_list = new ArrayList<String>(); 
		HashMap<String, Integer> event_idx = new HashMap<String, Integer>();

		HashMap<String, Integer> date_idx = new HashMap<String, Integer>();
		//read the event index csv
		CSVReader reader;
		try {
			reader = new CSVReader(new FileReader(args[0]), '\t');
			String[] entry;
			//remove the first row labels
			reader.readNext();
			while ((entry = reader.readNext()) != null) {
				if (entry.length < 3) continue;
				if (entry[1].trim().equals(category)) {

					if(!event_idx_list.contains(entry[2])) {
						event_idx_list.add(entry[2]);
					}

					event_idx.put(entry[2], Integer.parseInt(entry[0]));
				}
			}
			QuickSort.sort(event_idx_list);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//read the date index csv
		try {
			reader = new CSVReader(new FileReader(args[1]), '\t');
			String[] entry;
			//remove the first row labels
			reader.readNext();
			while ((entry = reader.readNext()) != null) {
				if (entry.length < 2) continue;

				date_idx.put(entry[1], Integer.parseInt(entry[0]));				
			}
			QuickSort.sort(event_idx_list);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("***5: start correlation");


		//start correlation
		//starting date of the time series
		starting_point = r.binner.parseDate("2007-12-10");
		int eid = 0;
		BufferedWriter bwr ;
		
		//TSU similarity 
		
		PageSimilarity tsu = new PageSimilarity();
		for (String main_event : trg_events) {
			int id = 0;
			int st;
			int et;
			LocalDate main_event_sd;

			StringBuffer b_sb = new StringBuffer();
			
			EventPeriod main_ep = event_map.get(main_event);
			EventPeriod s_ep = null;
			for (String s_event : event_idx_list) {			
				System.out.println("main_event:\t" + main_event + " : " + event_idx.get(main_event));
				System.out.println("s_event:\t" + s_event + " : " + event_idx.get(s_event));

				//check on plan type
				//date row of a matrix starts at row 2, row 1 is for labels
				s_ep = event_map.get(main_event);
				main_event_sd =  main_ep.startingDate;

				int r1 = date_idx.get(main_ep.startingDate.toString());
				int r2 = date_idx.get(main_ep.endingDate.toString());
				st = type.equals("u") ? r1 : r1 - d;
				et = r2 + d;

				//only interested in past events
				if (event_map.get(s_event) == null) continue;
				if (event_map.get(s_event).endingDate.compareTo(main_event_sd) > 0) continue;

				//double[] burst_levels = r.getBurstLevels(st, et, event_idx.get(s_event));
				double[] localmax = r.getLocalMaxima(st, et, event_idx.get(s_event));
				double[] maxmin = r.getMaxMin(st, et, event_idx.get(s_event));
				b_sb.append(s_event);
				b_sb.append("\t");
				b_sb.append(r.getBurstLevels_(main_event_sd, event_map.get(main_event).endingDate, st, et, event_idx.get(s_event)));
				b_sb.append("\t");
				b_sb.append(Arrays.toString(localmax));
				b_sb.append("\t");
				b_sb.append(Arrays.toString(maxmin));
				b_sb.append("\n");

                //filter by bursts
				/*
				double burst_density = r.filterByBursts(event_map.get(main_event).startingDate, event_map.get(main_event).endingDate, st, et, event_idx.get(s_event));
				double threshold = 0.5d;
				if (burst_density > threshold) {
					//TODO:
				}*/
				
				//TSU sim
				
				int[] ts = r.getTimeSeries(st, et, event_idx.get(s_event));
				//double tsu_score = tsu.TSUSim(main_event_sd, main_ep.endingDate, s_ep.startingDate, s_ep.endingDate, ts);
				
				



			}

			bwr = new BufferedWriter(new FileWriter(new File("/home/tunguyen/burst/" + main_event + "_en_" + type + "_d_" + d + "_sm_" + sm + "_lag_" + l + "_sse.csv")));
			bwr.write(b_sb.toString());
			bwr.flush();
			bwr.close();
		}



	}

	/**
	 * 
	 * @param args
	 * @throws ParseException
	 */
	public static void main_ (String[] args) throws ParseException {		

		RWikiEvents r = new RWikiEvents();
		r.init();
		System.out.println("***1");

		// create the command line parser
		CommandLineParser parser = new GnuParser();

		// create Options object
		Options options = new Options();

		options.addOption( "c", "category", true, "is the event category e.g., atlantic_hurricane_season" );
		options.addOption( "cf", "category-meta", true, "is the path to metadata event category file, contains event dates e.g., /atlantic_hurricane.csv" );
		options.addOption( "d", "day", true, "is the number of days used for sliding windows" );
		options.addOption( "l", "lag", true, "is the time lag used in cross-correlation" );
		options.addOption( "t", "type", true, "is the event type, e.g., u" );
		options.addOption( "sm", "smoothing", true, "smoothing parameter" );

		System.out.println("***2");

		// parse the command line arguments
		CommandLine line = parser.parse( options, args);
		//event type e.g., u
		String type = line.hasOption("t") ? line.getOptionValue("t") : "u";
		//event category
		String category = line.hasOption("c") ? line.getOptionValue("c") : "atlantic_hurricane_season";
		//path to event metadata file contains event date (csv)
		String c_path = line.hasOption("cf") ? line.getOptionValue("cf") : "/home/kanhabua/projects/deduplication/data/WikiEvents/rfeature_results_08_12_13/atlantic_hurricane.csv";

		//String c_path = line.hasOption("cf") ? line.getOptionValue("cf") : "C:/Users/Kanhabua/Desktop/rfeature_results_08_12_13/atlantic_hurricane.csv";

		//sliding window
		String d_ = line.hasOption("d") ? line.getOptionValue("d") : "14";
		int d = Integer.parseInt(d_);
		//lag
		String l_ = line.hasOption("l") ? line.getOptionValue("l") : "7";
		int l = Integer.parseInt(l_);  
		//sm
		String sm_ = line.hasOption("sm") ? line.getOptionValue("sm") : "1";
		double sm = Double.parseDouble(sm_);

		System.out.println("***3");

		//load event metatdata from csv file
		HashMap<String, EventPeriod> event_map = r.getEventMetadata_(c_path);
		Set<String> keyset = event_map.keySet();

		//triggering event list (after 2008)
		ArrayList<String> trg_events = new ArrayList<String>();
		LocalDate starting_point = r.binner.parseDate("2008-01-01");
		for(String expr : keyset){
			if(event_map.get(expr).startingDate.isAfter(starting_point) && !trg_events.contains(expr)) {
				trg_events.add(expr);
			}
		}
		QuickSort.sort(trg_events);

		System.out.println("***4");

		ArrayList<String> event_idx_list = new ArrayList<String>(); 
		HashMap<String, Integer> event_idx = new HashMap<String, Integer>();

		HashMap<String, Integer> date_idx = new HashMap<String, Integer>();
		//read the event index csv
		CSVReader reader;
		try {
			reader = new CSVReader(new FileReader(args[0]), '\t');
			String[] entry;
			//remove the first row labels
			reader.readNext();
			while ((entry = reader.readNext()) != null) {
				if (entry.length < 3) continue;
				if (entry[1].trim().equals(category)) {
					if (entry.length < 3) continue;
					if(!event_idx_list.contains(entry[2])) {
						event_idx_list.add(entry[2]);
					}

					event_idx.put(entry[2], Integer.parseInt(entry[0]));
				}
			}
			QuickSort.sort(event_idx_list);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//read the date index csv
		try {
			reader = new CSVReader(new FileReader(args[1]), '\t');
			String[] entry;
			//remove the first row labels
			reader.readNext();
			while ((entry = reader.readNext()) != null) {
				if (entry.length < 2) continue;
				date_idx.put(entry[1], Integer.parseInt(entry[0]));					
			}
			QuickSort.sort(event_idx_list);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("***5: start correlation");

		//start correlation
		//starting date of the time series
		starting_point = r.binner.parseDate("2007-12-10");
		int eid = 0;
		for (String main_event : trg_events) {
			int id = 0;
			int st;
			int et;
			LocalDate main_event_sd;

			//for sse
			StringBuffer s_sb = new StringBuffer();
			s_sb.append("colnames(sse_result" + eid + ") <- c(	");

			//for kurtosis
			StringBuffer k_sb = new StringBuffer();
			k_sb.append("colnames(kurtosis_result" + eid + ") <- c(	");

			//for correlation
			StringBuffer sb = new StringBuffer();
			sb.append("colnames(ccf_result" + eid + ") <- c(\"lag\",");

			for (String s_event : event_idx_list) {			
				System.out.println("main_event:\t" + main_event + " : " + event_idx.get(main_event));
				System.out.println("s_event:\t" + s_event + " : " + event_idx.get(s_event));

				//check on plan type
				//date row of a matrix starts at row 2, row 1 is for labels
				main_event_sd =  event_map.get(main_event).startingDate;

				int r1 = date_idx.get(event_map.get(main_event).startingDate.toString());
				int r2 = date_idx.get(event_map.get(main_event).endingDate.toString());
				st = type.equals("u") ? r1 : r1 - d;
				et = r2 + d;

				System.out.println("st[" + event_map.get(main_event).startingDate + "]=" + st + ", et[" + event_map.get(main_event).endingDate + "]=" + et);

				//only interested in past events
				if (event_map.get(s_event) == null) continue;
				if (event_map.get(s_event).endingDate.compareTo(main_event_sd) > 0) continue;
				//for correlation
				sb.append("\"" + s_event + "\",");

				//for kurtosis
				k_sb.append("\"" + s_event + "\",");

				//for sse
				s_sb.append("\"" + s_event + "\",");
				System.out.println(s_event);
				r.ccf(st, et, event_idx.get(main_event), event_idx.get(s_event), id, eid, l, sm);
				r.kurtosis_sse(st, et, event_idx.get(s_event), id++, eid);

			}

			//aggregate ccf results into a file
			StringBuffer sb2 = new StringBuffer();
			sb2.append("ccf_result" + eid + " <- cbind(");
			sb2.append("cv_wp" + eid + "_0[, 1],");

			for (int i = 0; i < id; i++) {
				if (i == id - 1) sb2.append("cv_wp" + eid + "_" + i + "[, 2])");
				else 
					sb2.append("cv_wp" + eid + "_" + i + "[, 2],");
			}

			r.engine.eval(sb2.toString());

			//aggregate kurtosis results into a file
			StringBuffer k_sb2 = new StringBuffer();
			k_sb2.append("kurtosis_result" + eid + " <- cbind(");

			for (int i = 0; i < id; i++) {
				if (i == id - 1) k_sb2.append("kv_wp" + eid + "_" + i + ")");
				else 
					k_sb2.append("kv_wp" + eid + "_" + i + ",");
			}
			r.engine.eval(k_sb2.toString());

			//aggregate sse results into a file
			StringBuffer s_sb2 = new StringBuffer();
			s_sb2.append("sse_result" + eid + " <- cbind(");

			for (int i = 0; i < id; i++) {
				if (i == id - 1) s_sb2.append("sv_wp" + eid + "_" + i + "$SSE)");
				else 
					s_sb2.append("sv_wp" + eid + "_" + i + "$SSE,");
			}

			r.engine.eval(s_sb2.toString());

			//correlation
			String sb_ = sb.toString();
			sb_ = sb_.substring(0, sb_.length() - 1);
			sb_ = sb_ + ")";
			r.engine.eval(sb_);

			//kurtosis
			String ksb_ = k_sb.toString();
			ksb_ = ksb_.substring(0, ksb_.length() - 1);
			ksb_ = ksb_ + ")";
			r.engine.eval(ksb_);

			//sse
			String ssb_ = s_sb.toString();
			ssb_ = ssb_.substring(0, ssb_.length() - 1);
			ssb_ = ssb_ + ")";
			r.engine.eval(ssb_);


			//Sort ccf values by lag = 0, 1, 2, 3 (descending)		
			r.engine.eval("t_ccf_result" + eid + " <- t(ccf_result" + eid + ")");

			// if lag >1 do ... else ... TODO:
			//lag = 7
			int l1 = l + 1;
			int l2 = l + 2;
			int l3 = l + 3;
			r.engine.eval("sorted_ccf_result" + eid + " <- t_ccf_result" + eid + "[order(-t_ccf_result" + eid + "[,"+ l1 +"], -t_ccf_result" + eid + "[,"+ l2 +"], -t_ccf_result" + eid + "[,"+ l3 +"]) , ]");

			//r.engine.eval("a = paste(\"home/tunguyen/correlation/\"atlantic_hurricane_season_en\", \"_\", \"u\", \"_d\", 14, \"_sm\", 1, \"_lag\", 7, \"_ccf.csv\", sep=\"\")");
			r.engine.eval("write.csv(sorted_ccf_result" + eid + ", \"/home/kanhabua/projects/deduplication/data/WikiEvents/rfeature_results_08_12_13/correlation/" + main_event + "_en_" + type + "_d_" + d + "_sm_" + sm + "_lag_" + l + "_ccf.csv\")");
			//r.engine.eval("write.csv(sorted_ccf_result" + eid + ", \"C:/Users/Kanhabua/Desktop/rfeature_results_08_12_13/correlation/" + main_event + "_en_" + type + "_d_" + d + "_sm_" + sm + "_lag_" + l + "_ccf.csv\")");


			//kurtosis	
			r.engine.eval("t_kurtosis_result" + eid + " <- t(kurtosis_result" + eid + ")");
			r.engine.eval("sorted_kurtosis_result" + eid + "  <- t_kurtosis_result" + eid + " [order(-t_kurtosis_result" + eid + " [,1]),]");
			r.engine.eval("write.csv(sorted_kurtosis_result" + eid + ", \"/home/kanhabua/projects/deduplication/data/WikiEvents/rfeature_results_08_12_13/kurtosis/" + main_event + "_en_" + type + "_d_" + d + "_sm_" + sm + "_lag_" + l + "_kurtosis.csv\")");
			//r.engine.eval("write.csv(sorted_kurtosis_result" + eid + ", \"C:/Users/Kanhabua/Desktop/rfeature_results_08_12_13/kurtosis/" + main_event + "_en_" + type + "_d_" + d + "_sm_" + sm + "_lag_" + l + "_kurtosis.csv\")");

			//sse

			r.engine.eval("t_sse_result" + eid + " <- t(sse_result" + eid + ")");
			r.engine.eval("sorted_sse_result" + eid + "  <- t_sse_result" + eid + " [order(-t_sse_result" + eid + " [,1]),]");
			r.engine.eval("write.csv(sorted_sse_result" + eid + ", \"/home/kanhabua/projects/deduplication/data/WikiEvents/rfeature_results_08_12_13/sse/" + main_event + "_en_" + type + "_d_" + d + "_sm_" + sm + "_lag_" + l + "_sse.csv\")");
			//r.engine.eval("write.csv(sorted_sse_result" + eid + ", \"C:/Users/Kanhabua/Desktop/rfeature_results_08_12_13/sse/" + main_event + "_en_" + type + "_d_" + d + "_sm_" + sm + "_lag_" + l + "_sse.csv\")");

			eid++;


		}

	}


}




