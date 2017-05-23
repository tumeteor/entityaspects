package l3s.de.event.wiki;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;

import l3s.de.rjava.TextConsole;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.google.common.primitives.Ints;

import de.l3s.tools.Burst;
import de.l3s.tools.BurstCollector;
import de.l3s.tools.BurstDetector;
import de.l3s.tools.LocalDateBinner;
import de.l3s.tools.SimpleBurstCollector;
import de.l3s.tools.ThresholdingCollector;


import au.com.bytecode.opencsv.CSVReader;

public class RWikiEvents3 {
	public Rengine engine;
	public static LocalDateBinner binner = new LocalDateBinner();
	public String startingDate = "Starting Date";
	public String endingDate = "Ending Date";
	//last date of the view logs
	public static final LocalDate LAST_DATE = binner.parseDate("2013-07-30");
	//first date of the view logs
	public static final LocalDate FIRST_DATE = binner.parseDate("2007-12-15");

	//starting date for the triggering event
	public static LocalDate startingTriggerDate = binner.parseDate("2008-01-01");

	//starting valid dates for R feature extraction: first/last available view date +/- 14 days)        		
	static LocalDate startingViewDate = binner.parseDate("2007-12-10").plusWeeks(2);
	static LocalDate endingViewDate = binner.parseDate("2013-07-31").minusWeeks(2);

	private final double densityScaling =  1.5;
	private final double gamma = 1.0 ;
	private final int states = 1;
	private final double threshold = 0.0;

	public RWikiEvents3() {
		//System.setProperty("R_HOME", "/usr/bin/R");

		String[] rOptions = { "--vanilla" };
		engine = new Rengine(rOptions, false, new TextConsole());
		engine.eval("library(e1071)");
		engine.eval("library(lsa)");	
		engine.eval("library(TTR)");

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

	public void init(String matrixFile) {
		System.out.println("***begin init()");

		//engine.eval("path = paste(\"/home/kanhabua/projects/deduplication/data/WikiEvents/matrix/\", " + csv_file + ", sep=\"\")");
		//engine.eval("all_div <- read.csv(" + csv_file + ", header=T, sep='\t'");	
		engine.eval("all_div <- read.csv(\"" + matrixFile + "\", header=T, sep=\"\t\")");
		engine.eval("source('/home/kanhabua/projects/deduplication/data/WikiEvents/rfeature_results_08_12_13/ccf.r')");
		//engine.eval("all_div <- read.csv(\"K:/deduplication/matrix/all_events_en_transposed.txt\", header=T, sep=\"\t\")");
		//engine.eval("source(\"C:/Users/Kanhabua/Desktop/atlantic_hurricane/ccf.r\")");

		System.out.println("***end init()");
	}

	public void createTS(int eid, int st, int et, int sc, double sm) {		
		engine.eval("wp_" + eid + " <- all_div[" + st + ":" + et + ", c(" + sc + ")]");
		engine.eval("wp_ts_" + eid + " <- ts(wp_" + eid + ")");		
		engine.eval("wp_ts_sm_" + eid + " <- SMA(wp_ts_" + eid + ", " + sm + ")");

		System.out.println("wp_" + eid + " <- all_div[" + st + ":" + et + ", c(" + sc + ")]");
		System.out.println("wp_ts_" + eid + " <- ts(wp_" + eid + ")");
		System.out.println("wp_ts_sm_" + eid + " <- SMA(wp_ts_" + eid + ", " + sm + ")");
	}

	public void removeTS(int eid) {		
		engine.eval("rm(wp_" + eid + ")");
		engine.eval("rm(wp_ts_" + eid + ")");		
		engine.eval("rm(wp_ts_sm_" + eid + ")");

		System.out.println("rm(wp_" + eid + ")");
		System.out.println("rm(wp_ts_" + eid + ")");		
		System.out.println("rm(wp_ts_sm_" + eid + ")");
	}

	public void ccf(int eid1, int eid2, int lag) {
		System.out.println("In ccf() eid1=" + eid1 + ", eid2=" + eid2 + ", lag=" + lag);
		engine.eval("cv_wp_" + eid1 + "_" + eid2 + " = ccf.tab(wp_ts_sm_" + eid1 + ", wp_ts_sm_" + eid2 + ", lag.max=" + lag + ", plot = FALSE, ylab = \"cross-correlation\", na.action=na.pass)");

		System.out.println("\tcv_wp_" + eid1 + "_" + eid2 + " = ccf.tab(wp_ts_sm_" + eid1 + ", wp_ts_sm_" + eid2 + ", lag.max=" + lag + ", plot = FALSE, ylab = \"cross-correlation\", na.action=na.pass)");		
	}

	public void kurtosis(int eid) {
		System.out.println("In kurtosis() eid=" + eid);

		// For kurtosis, use time series without smoothing
		REXP x = engine.eval("wp_ts_" + eid);
		engine.eval("kv_wp_" + eid + " = kurtosis(wp_ts_" + eid + ")");

		System.out.println("\twp_ts_" + eid + " = " + Arrays.toString(x.asDoubleArray()));		
		System.out.println("\tkv_wp_" + eid + " = kurtosis(wp_ts_" + eid + ")");
	}

	public void sse(int eid) {
		System.out.println("In sse() eid=" + eid);

		// For sse, use time series without smoothing
		// Note, the difference of time window with ccf/kurtosis 
		REXP x = engine.eval("wp_ts_" + eid);
		engine.eval("sv_wp_" + eid + " = HoltWinters(wp_ts_" + eid + ", gamma=FALSE)");

		System.out.println("\twp_ts_" + eid + " = " + Arrays.toString(x.asDoubleArray()));	
		System.out.println("\tsv_wp_" + eid + " = HoltWinters(wp_ts_" + eid + ", gamma=FALSE)");		
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

	public double[] getMaxMinMean(int st, int et, int sc) {
		engine.eval("wp <- all_div[" + st + ":" + et + ", c(" + sc + ")]");
		REXP x = engine.eval("max(wp)");
		REXP y = engine.eval("min(wp)");
		REXP z = engine.eval("mean(wp)");

		return new double[] {x.asDoubleArray()[0], y.asDoubleArray()[0], z.asDouble()};

	}

	/**
	 * Get timeseries
	 */
	public int[] getTimeSeries(int st, int et, int sc){
		REXP x = engine.eval("all_div[" + st + ":" + et + ", c(" + sc + ")]");

		//can only extract as double array even though its int array
		double[] ts_ = x.asDoubleArray();
		int[] ts = new int[ts_.length];
		for (int i = 0; i < ts.length; i++) {
			ts[i] = (int) ts_[i];
			System.out.print(ts[i] + " ");
		}
		System.out.println("");

		return ts;
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

	public double filterByBursts(LocalDate sd, LocalDate ed, int st, int et, int sc) {
		System.out.println("In filterByBursts() [" + st + " : " + et + " : " + sc + "]");

		LocalDate start = sd.minusMonths(1);
		LocalDate end = ed.plusMonths(1);
		//adapt the the time range of the view logs
		if (start.compareTo(FIRST_DATE) < 0) start = FIRST_DATE;
		if (end.compareTo(LAST_DATE) > 0) end = LAST_DATE;
		//fixed: just to make sure the number of days plus or minus ~30
		st = (st < Days.daysBetween(sd, start).getDays()) ? 0 : st - Days.daysBetween(sd, start).getDays();
		et = et + Days.daysBetween(ed, end).getDays();

		System.out.println("\t new " + st + " : " + et);
		//one month before and one month after the event
		Interval eventP = new Interval(start.toDateTime(new DateTime(0, 1, 1, 0, 0)),
				end.toDateTime(new DateTime(0, 12, 31, 23, 59)));

		int[] ts = getTimeSeries(st, et, sc);

		int max = Ints.max(ts);
		if (max == 0) return 0.0d;
		Range<LocalDate> timeRange = Ranges.closed(start, end);
		SimpleBurstCollector<String,LocalDate> collector = null;
		try {
			collector = detectTermBursts(ts, timeRange, max);
		} catch (java.lang.IllegalArgumentException iae) {}
		if (collector != null) {
			int i = 0;
			int count = 0;
			for(Burst<String, LocalDate> burst:collector.getBursts()){
				try{
					System.out.println("[" + count + "] Strength=" + burst.getStrength());
					System.out.println("\tStart=" + burst.getStart().toString() + "\tEnd=" + burst.getEnd().toString());
					System.out.println("\tPeriod=" + burst.getDuration().toInterval());			
					System.out.println("\tDay=" + burst.getDuration().toPeriod().getDays());

					i = burst.getDuration().overlaps(eventP) ? i + 1 : i;

					System.out.println("\tOverlap?" + burst.getDuration().overlaps(eventP) + " : i = " + i);


				} catch (java.lang.IllegalArgumentException iae){}
				count++;

			}
			return (double) i / collector.getBursts().size();
		} else {
			return 0.0d;
		}
	}

	/**
	 * Load event metadata from csv file
	 * sort by starting date (ascending)
	 * @param metadataFilePath
	 * @return
	 */
	public TreeMap<String, EventPeriod>  getEventMetadata(String metadataFilePath, HashMap<String, EventPeriod> expr_map) {
		//HashMap<String, EventPeriod> expr_map = new HashMap<String, EventPeriod>();
		/*CSVReader reader;
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
		}*/
		try{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(metadataFilePath), "UTF8"));
			String line = "";
			int st_i = 0;
			int et_i = 0;
			int lineCount = 0;
			while((line = br.readLine()) != null) {				    	
				String[] entry = line.split("\t");

				if(line.startsWith("#")) {
					continue;
				}

				if(lineCount == 0) {
					for(int i=0; i<entry.length; i++) {
						if(entry[i].equals(startingDate)) {
							st_i = i;
						}

						if(entry[i].equals(endingDate)) {
							et_i = i;
						}
					}
				} else {
					// assume date format: yyyy-mm-dd
					if(entry[st_i].length() > 10) {
						entry[st_i] = entry[st_i].replaceAll("\\s+", "");
					}
					if(entry[et_i].length() > 10) {
						entry[et_i] = entry[st_i].replaceAll("\\s+", "");
					}
					System.out.println("[" + lineCount + "] " + entry[0] + "\t" + entry[st_i] + "\t" + entry[et_i]);

					expr_map.put(entry[0], new EventPeriod(binner.parseDate(entry[st_i]),
							binner.parseDate(entry[et_i])));
				}

				lineCount++;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//sorting by event starting date
		//System.out.println("expr_map.size1=" + expr_map.size());
		MapValueComparator bvc =  new MapValueComparator(expr_map);
		TreeMap<String, EventPeriod> sorted_map = new TreeMap<String, EventPeriod>(bvc);

		sorted_map.putAll(expr_map);

		NavigableSet<String> keyset = sorted_map.descendingKeySet();
		System.out.println("1. sorted_map.size() = " + keyset.size());
		int e = 0;
		for (String event : keyset) {

			EventPeriod period2 = expr_map.get(event);
			System.out.println("\t[" + e + "] " + event + " : " + period2.startingDate.toString() + ", " + period2.endingDate.toString());
			e++;
		}

		//System.out.println("sorted_map =" + sorted_map);

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
		/*CSVReader reader;
		try {
			reader = new CSVReader(new InputStreamReader(getClass().getResourceAsStream(metadataFilePath)), '\t');
			String[] entry;
			while ((entry = reader.readNext()) != null) {
				expr_map.put(entry[0], new EventPeriod(binner.parseDate(entry[1]),
				binner.parseDate(entry[2])));
			}*/
		try{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(metadataFilePath), "UTF8"));
			String line = "";
			int st_i = 0;
			int et_i = 0;
			int lineCount = 0;
			while((line = br.readLine()) != null) {				    	
				String[] entry = line.split("\t");
				if(lineCount == 0) {
					for(int i=0; i<entry.length; i++) {
						if(entry[i].equals(startingDate)) {
							st_i = i;
						}

						if(entry[i].equals(endingDate)) {
							et_i = i;
						}
					}
				} else {
					System.out.println(entry[0] + "\t" + entry[st_i] + "\t" + entry[et_i]);

					expr_map.put(entry[0], new EventPeriod(binner.parseDate(entry[st_i]),
							binner.parseDate(entry[et_i])));
				}

				lineCount++;
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
			//return base.get(a).startingDate.compareTo(base.get(b).startingDate);

			if (base.get(a).startingDate.compareTo(base.get(b).startingDate) >= 0) {
				return 1;
			} else {
				return -1;
			} // returning 0 would merge keys
		}
	}

	public void clearMainMatrix() {		
		this.engine.eval("rm(all_div)");	
	}		

	public void endEngine(){
		this. engine.end();
	}

	/**
	 * 
	 * @param args
	 * @throws ParseException
	 */
	public static void main (String[] args) throws ParseException {		

		String matrixFile = "/home/kanhabua/projects/deduplication/data/WikiEvents/matrix/all_events_en_transposed.txt";
		RWikiEvents3 r = new RWikiEvents3();
		r.init(matrixFile);

		// create the command line parser
		CommandLineParser parser = new GnuParser();

		// create Options object
		Options options = new Options();

		options.addOption( "c", "categories", true, "is a directory path to event categories and metadata" );
		//options.addOption( "c", "category", true, "is the event category e.g., atlantic_hurricane_season" );
		//options.addOption( "cf", "category-meta", true, "is the path to metadata event category file, contains event dates e.g., /atlantic_hurricane.csv" );
		options.addOption( "d", "day", true, "is the number of days used for sliding windows" );
		options.addOption( "l", "lag", true, "is the time lag used in cross-correlation" );
		//options.addOption( "t", "type", true, "is the event type, e.g., u" );
		options.addOption( "sm", "smoothing", true, "smoothing parameter" );	
		options.addOption( "vd", "viewdate", true, "using the first view date as the starting date" );
		options.addOption( "f", "filter", true, "using burst density to filter related events" );

		// parse the command line arguments
		CommandLine line = parser.parse( options, args);

		//event type e.g., u
		//String type = line.hasOption("t") ? line.getOptionValue("t") : "u";

		//event category
		//String category = line.hasOption("c") ? line.getOptionValue("c") : "atlantic_hurricane_season";
		String categoryFolder = line.hasOption("c") ? line.getOptionValue("c") : "/home/kanhabua/projects/deduplication/data/WikiEvents/annotation/final/event_categories/";		

		//path to event metadata file contains event date (csv)
		//String c_path = line.hasOption("cf") ? line.getOptionValue("cf") : "/home/kanhabua/projects/deduplication/data/WikiEvents/rfeature_results_08_12_13/atlantic_hurricane.csv";

		//String c_path = line.hasOption("cf") ? line.getOptionValue("cf") : "C:/Users/Kanhabua/Desktop/rfeature_results_08_12_13/atlantic_hurricane.csv";

		//sliding window
		String d_ = line.hasOption("d") ? line.getOptionValue("d") : "14";
		int d = Integer.parseInt(d_);
		//lag
		String l_ = line.hasOption("l") ? line.getOptionValue("l") : "7";
		int l = Integer.parseInt(l_);  
		//sm
		String sm_ = line.hasOption("sm") ? line.getOptionValue("sm") : "1";
		int sm = Integer.parseInt(sm_);

		// Determine a starting date from the first date with non-zero view (applicable for unplan events)
		String vd_ = line.hasOption("vd") ? line.getOptionValue("vd") : "false";
		boolean vd = Boolean.parseBoolean(vd_);

		// Using burst density to filter related events
		String f_ = line.hasOption("f") ? line.getOptionValue("f") : "false";
		boolean filter = Boolean.parseBoolean(f_);

		//(1) sm=1
		//Case 1.1: d=14, sm=1, lag=7 		
		//Case 1.2: d=7, sm=1, lag=3
		//Case 1.3: d=3, sm=1, lag=1

		//(2) sm=3
		//Case 2.1: d=14, sm=3, lag=7 
		//Case 2.2: d=7, sm=3, lag=3
		//Case 2.3: d=3, sm=3, lag=1

		//(3) sm=7
		//Case 3.1: d=14, sm=7, lag=7 
		//Case 3.2: d=7, sm=7, lag=3
		//Case 3.3: d=3, sm=7, lag=1		

		String outputDir = "/home/kanhabua/projects/deduplication/data/WikiEvents/rfeature_results_131213_d_" + d + "_sm_" + sm + "_lag_" + l;
		if(vd) {
			outputDir = outputDir + "_vd";
		}

		if(filter) {
			outputDir = outputDir + "_f";
		}

		// Iterate over all categories
		File inputFile = new File(categoryFolder);		
		if(inputFile.exists() && inputFile.isDirectory()){

			File[] files = inputFile.listFiles();
			Arrays.sort(files);
			int noOfFiles = files.length;
			for(int f = 0; f<noOfFiles; f++){
				String c_file = files[f].getName();
				String c_path = files[f].getAbsolutePath(); 
				String category = c_file.substring(0, c_file.indexOf("."));
				String type = r.getEventType(category);

				System.out.println("===================== Category[" + category + ", type=" + type + "] =====================");

				//load event metatdata from csv file
				//HashMap<String, EventPeriod> event_map = r.getEventMetadata_(c_path);
				HashMap<String, EventPeriod> event_map = new HashMap<String, EventPeriod>();
				TreeMap<String, EventPeriod> sorted_event_map = r.getEventMetadata(c_path, event_map);

				ArrayList<String> all_events_list = new ArrayList<String>();
				NavigableSet<String> keyset = sorted_event_map.descendingKeySet();
				System.out.println("2.sorted_map.size() = " + keyset.size());

				int j = 0;
				System.out.println("------------- Begin list of events ---------------");
				for (String event : keyset) {

					all_events_list.add(event);

					// Get EventPeriod from event_map, not sorted_event_map!
					// thi si not null 
					System.out.println(j + " : " + event + " : " + event_map.get(event).startingDate + " : " + event_map.get(event).endingDate);
					j++;
				}
				System.out.println("------------- End list of events ---------------");

				//triggering event list (after 2008)
				//ArrayList<String> trg_events = new ArrayList<String>();

				//LocalDate starting_point = r.binner.parseDate("2008-01-01");
				//for(String expr : keyset){
				//	if(event_map.get(expr).startingDate.isAfter(starting_point) && !trg_events.contains(expr)) {
				//		trg_events.add(expr);
				//	}

				//	all_events.add(expr);
				//}
				// Don't sort by alphabet, but starting time
				//QuickSort.sort(trg_events);		

				//HashMap<String, Integer> event_idx = new HashMap<String, Integer>();
				HashMap<String, Integer> column_idx = new HashMap<String, Integer>();

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

						//System.out.println("entry=" + entry.toString());

						// Only consider Wikipedia articles belonging to the current category
						// 11.12.13: using 'contains' instead of 'equals' for the categories "atlantic_hurricane_season(s)"
						if (entry[1].trim().contains(category)) {
							String event = entry[2];

							// Ignore non-event articles and those with no event profiles available 
							//System.out.println("event_idx_list.contains(event)=" + event_idx_list.contains(event) + "\tevent_map.containsKey(event)=" + event_map.containsKey(event));
							//System.out.println("\t1.Adding event=" + event + "\tc=" + Integer.parseInt(entry[0]));
							column_idx.put(event, Integer.parseInt(entry[0]));        														
						} //else {
						//	System.out.println(entry[1].trim() + "!=" + category);
						//}
					}

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
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				//start correlation

				//int eid = 0;
				int allEvents = all_events_list.size();
				LocalDate st_event;
				LocalDate et_event;

				//for (String main_event : trg_events) {
				System.out.println("==== Start extracting R features ====");
				for (int eid = 0; eid < allEvents; eid++) {
					String event = all_events_list.get(eid);
					if(event_map.get(event) == null) {
						System.out.println("Error: event_map.get(" + event + ") is null");
						continue;
					}        			

					if(!column_idx.containsKey(event)) {
						System.out.println("Error: no column index for " + event);
						continue;
					}        			

					System.out.println("Current event: [" + eid + "] " + event);

					if(event_map.get(event).startingDate.compareTo(startingViewDate) >= 0) {
						System.out.println("\t\t1. " + event_map.get(event).startingDate + " compareTo " + startingViewDate + " is greater than or equal: " + event_map.get(event).startingDate.compareTo(startingViewDate));
						st_event = event_map.get(event).startingDate;
					} else {
						System.out.println("\t\t2. " + event_map.get(event).startingDate + " compareTo " + startingViewDate + " is less than " + event_map.get(event).startingDate.compareTo(startingViewDate));        				
						st_event = startingViewDate;
					}

					if(event_map.get(event).endingDate.compareTo(endingViewDate) <= 0) {
						System.out.println("\t\t3. " + event_map.get(event).endingDate + " compareTo " + endingViewDate + " is less than or equal: " + event_map.get(event).endingDate.compareTo(endingViewDate));        				
						et_event = event_map.get(event).endingDate;        				
					} else {
						System.out.println("\t\t4. " + event_map.get(event).endingDate + " compareTo " + endingViewDate + " is greater: " + event_map.get(event).endingDate.compareTo(endingViewDate));        				        				
						et_event = endingViewDate;
					}

					System.out.println("st[" + st_event + "], et[" + et_event + "], c[" + column_idx.get(event) + "]");

					boolean isTriggeringEvent = st_event.isAfter(startingTriggerDate);

					if(!isTriggeringEvent) {
						System.out.println("\tSkip this event.");
						continue;
					}

					final long startTime1 = System.currentTimeMillis();        			

					int st1, et1; // for ccf and kurtosis
					int st2, et2; // for sse

					int r1, r2;

					if(!vd) {
						// (1) Get starting date from annotation
						// Row index starting from 1 (not 2), but excluding header!!!    					
						if(date_idx.get(st_event.toString()) == null) {
							System.out.println("1. date_idx.get(" + st_event.toString() + ") is null");
						}

						if(date_idx.get(et_event.toString()) == null) {
							System.out.println("2. date_idx.get(" + et_event.toString() + ") is null");
						}
						r1 = date_idx.get(st_event.toString());
						r2 = date_idx.get(et_event.toString());
					} else {
						// (2) Get starting date from the first date of non-zero views
						REXP x = r.engine.eval("all_div[ 1:2061, c(" + column_idx.get(event) + ")]");
						double[] event_ts = x.asDoubleArray();
						// i is the starting date index
						int i;
						for (i = 0 ; i < event_ts.length; i++) {
							if (event_ts[i] > 0) break;
						}

						r1 = i+1;
						r2 = date_idx.get(et_event.toString());	        				
					}

					// A time periods used for cross correlation is different for "planned" and "unplanned" events
					if(type.equals("p")) {
						st1 = r1 - d;
						et1 = r2 + d;
					} else  {
						st1 = r1;
						et1 = r2 + d;
					}

					// Only consider a time period "before (+1 day) the event" for surprise detection
					st2 = r1 - d;
					et2 = r1 + sm;

					// Check whether the row index is negative
					if(st1 <=0 ) {
						st1 = 1;
						System.out.println("\tst1[" + st1 + "] has a negative value.");
					}

					if(st2 <=0 ) {
						st2 = 1;
						System.out.println("\tst2[" + st2 + "] has a negative value.");
					}

					if(et1 <=0 ) {
						et1 = 1;
						System.out.println("\tet1[" + et1 + "] has a negative value.");
					}

					if(et2 <=0 ) {
						et2 = 1;
						System.out.println("\tet2[" + et2 + "] has a negative value.");
					}

					// create time series data for the main event
					r.createTS(eid, st1, et1, column_idx.get(event), sm);

					//for sse
					StringBuffer s_sb = new StringBuffer();
					s_sb.append("colnames(sse_result" + eid + ") <- c(	");

					//for kurtosis
					StringBuffer k_sb = new StringBuffer();
					k_sb.append("colnames(kurtosis_result" + eid + ") <- c(	");

					//for correlation
					StringBuffer sb = new StringBuffer();
					sb.append("colnames(ccf_result" + eid + ") <- c(\"lag\",");

					//for (String s_event : event_idx.keySet()) {
					// Extract R features for all other events occurred before the main event, i.e., all-pair comparison
					// Note, 1) assume that the list are sorted descending by starting dates 
					for (int eid2 = eid + 1; eid2 < allEvents; eid2++) {
						String s_event = all_events_list.get(eid2);
						if(event_map.get(s_event) == null) {
							System.out.println("\tError: event_map.get(" + s_event + ") is null");
							continue;
						}  

						System.out.println("\tRelated event: [" + eid2 + "] " + s_event);

						if(!column_idx.containsKey(s_event)) {
							System.out.println("\tError: no column index for " + s_event);
							continue;
						}

						LocalDate st_s_event;
						LocalDate et_s_event;

						if(event_map.get(s_event).startingDate.compareTo(startingViewDate) >= 0) {
							System.out.println("\t\t5. " + event_map.get(s_event).startingDate + " compareTo " + startingViewDate + " is greater than or equal: " + event_map.get(s_event).startingDate.compareTo(startingViewDate));            				            			
							st_s_event = event_map.get(s_event).startingDate;
						} else {
							System.out.println("\t\t6. " + event_map.get(s_event).startingDate + " compareTo " + startingViewDate + " is less than: " + event_map.get(s_event).startingDate.compareTo(startingViewDate));            				            			
							st_s_event = startingViewDate;
						}

						if(event_map.get(s_event).endingDate.compareTo(endingViewDate) <= 0) {
							System.out.println("\t\t7. " + event_map.get(s_event).endingDate + " compareTo " + endingViewDate + " is less than or equal: " + event_map.get(s_event).endingDate.compareTo(endingViewDate));            				            			            			
							et_s_event = event_map.get(s_event).endingDate;
						} else {
							System.out.println("\t\t8. " + event_map.get(s_event).endingDate + " compareTo " + endingViewDate + " greater " + event_map.get(s_event).endingDate.compareTo(endingViewDate));            				
							et_s_event = endingViewDate;
						}

						final long startTime2 = System.currentTimeMillis();            			
						System.out.println("\tst[" + st_s_event + "], et[" + et_s_event + "], c[" + column_idx.get(s_event) + "]");

						// Filter by bursts and aggregate numbers during the computing of combined scores
						if(filter) {
							double burst_density = r.filterByBursts(st_event, et_event, st1, et1, column_idx.get(s_event));
							double threshold = 0.5d;
							if (burst_density < threshold) {
								System.out.println("\t\tSkip related event: [" + eid2 + "] " + s_event + " : " + burst_density);
							}

							double[] aggregate = r.getMaxMinMean(st1, et1, column_idx.get(s_event));
							System.out.println("\taggregate[min, max, mean] = " + aggregate[0] + " : " + aggregate[1] + " : " + aggregate[2]);

							continue;
						}

						// create time series data for a related event
						// for ccf, kurtosis
						r.createTS(eid2, st1, et1, column_idx.get(s_event), sm);        				

						//check on plan type
						//date row of a matrix starts at row 2, row 1 is for labels
						//main_event_sd =  event_map.get(main_event).startingDate;

						//only interested in past events
						// The following conditions will be replaced with the sorted list and for-loop
						//if (event_map.get(s_event) == null) continue;
						//if (event_map.get(s_event).endingDate.compareTo(main_event_sd) > 0) continue;

						//for correlation
						sb.append("\"" + s_event + "\",");

						//for kurtosis
						k_sb.append("\"" + s_event + "\",");

						//for sse
						s_sb.append("\"" + s_event + "\",");
						//System.out.println(s_event);

						// ccf
						final long startTime3 = System.currentTimeMillis();            			
						//r.ccf(st1, et1, event_idx.get(main_event), event_idx.get(s_event), id, eid, l, sm);
						r.ccf(eid, eid2, l);
						final long endTime3 = System.currentTimeMillis();
						System.out.println("\tTotal execution time (ccf): " + (endTime3 - startTime3) );

						// kurtosis
						final long startTime4 = System.currentTimeMillis();            			
						//r.kurtosis(st1, et1, event_idx.get(s_event), id, eid);        	            
						r.kurtosis(eid2);
						final long endTime4 = System.currentTimeMillis();
						System.out.println("\tTotal execution time (kurtosis): " + (endTime4 - startTime4) );

						// sse
						// clear time series variable and create with the new time period
						r.removeTS(eid2);
						r.createTS(eid2, st2, et2, column_idx.get(s_event), sm);        				        				
						final long startTime5 = System.currentTimeMillis();            			
						//r.sse(st2, et2, event_idx.get(s_event), id, eid);
						r.sse(eid2);
						final long endTime5 = System.currentTimeMillis();
						System.out.println("\tTotal execution time (sse): " + (endTime5 - startTime5) );

						//id++;

						final long endTime2 = System.currentTimeMillis();
						System.out.println("Total execution time (the main event): " + (endTime2 - startTime2) );

						// ## Clear time series variables ##
						r.removeTS(eid2);
					}

					//No results of ccf, kurtosis, sse for filter
					if(filter) {
						continue;
					}

					//aggregate ccf results into a file
					StringBuffer sb2 = new StringBuffer();
					sb2.append("ccf_result" + eid + " <- cbind(");

					//for (int i = 0; i < id; i++) {
					for (int eid2 = eid + 1; eid2 < allEvents; eid2++) {
						if(eid2 == eid + 1) sb2.append("cv_wp_" + eid + "_" + eid2 + "[, 1],");

						if (eid2 == allEvents - 1) sb2.append("cv_wp_" + eid + "_" + eid2 + "[, 2])");
						else 
							sb2.append("cv_wp_" + eid + "_" + eid2 + "[, 2],");
					}

					r.engine.eval(sb2.toString());

					//aggregate kurtosis results into a file
					StringBuffer k_sb2 = new StringBuffer();
					k_sb2.append("kurtosis_result" + eid + " <- cbind(");


					//for (int i = 0; i < id; i++) {
					for (int eid2 = eid + 1; eid2 < allEvents; eid2++) {
						if (eid2 == allEvents - 1) k_sb2.append("kv_wp_" + eid2 + ")");
						else 
							k_sb2.append("kv_wp_" + eid2 + ",");
					}
					r.engine.eval(k_sb2.toString());

					//aggregate sse results into a file
					StringBuffer s_sb2 = new StringBuffer();
					s_sb2.append("sse_result" + eid + " <- cbind(");

					//for (int i = 0; i < id; i++) {
					for (int eid2 = eid + 1; eid2 < allEvents; eid2++) {
						if (eid2 == allEvents - 1) s_sb2.append("sv_wp_" + eid2 + "$SSE)");
						else 
							s_sb2.append("sv_wp_" + eid2 + "$SSE,");
					}
					r.engine.eval(s_sb2.toString());

					//correlation
					String sb_ = sb.toString();
					sb_ = sb_.substring(0, sb_.length() - 1);
					sb_ = sb_ + ")";        			
					System.out.println("*** sb_ = " + sb_);        			
					r.engine.eval(sb_);

					//kurtosis
					String ksb_ = k_sb.toString();
					ksb_ = ksb_.substring(0, ksb_.length() - 1);
					ksb_ = ksb_ + ")";
					System.out.println("*** ksb_ = " + ksb_);        			        			
					r.engine.eval(ksb_);

					//sse
					String ssb_ = s_sb.toString();
					ssb_ = ssb_.substring(0, ssb_.length() - 1);
					ssb_ = ssb_ + ")";
					System.out.println("*** ssb_ = " + ssb_);        			        			        			
					r.engine.eval(ssb_);


					//Sort ccf values by lag = 0, 1, 2, 3 (descending)		
					r.engine.eval("t_ccf_result" + eid + " <- t(ccf_result" + eid + ")");

					String outputDir_ccf = outputDir + "/" + category + "/correlation/";
					File outputFile = new File(outputDir_ccf);
					if(!outputFile.exists()) {
						outputFile.mkdirs();
					}

					String outputDir_kurtosis = outputDir + "/" + category + "/kurtosis/";
					outputFile = new File(outputDir_kurtosis);
					if(!outputFile.exists()) {
						outputFile.mkdirs();
					}

					String outputDir_sse = outputDir + "/" + category + "/sse/";
					outputFile = new File(outputDir_sse);
					if(!outputFile.exists()) {
						outputFile.mkdirs();
					}

					String outputFile_ccf = outputDir_ccf + event + "_en_" + type + "_d_" + d + "_sm_" + sm + "_lag_" + l + "_ccf";
					String outputFile_kurtosis = outputDir_kurtosis + event + "_en_" + type + "_d_" + d + "_sm_" + sm + "_lag_" + l + "_kurtosis";
					String outputFile_sse = outputDir_sse + event + "_en_" + type + "_d_" + d + "_sm_" + sm + "_lag_" + l + "_sse";

					if(vd) {    	    		
						outputFile_ccf = outputFile_ccf + "_vd";
						outputFile_kurtosis = outputFile_kurtosis + "_vd";
						outputFile_sse = outputFile_sse + "_vd";
					}

					if(filter) {    	    		
						outputFile_ccf = outputFile_ccf + "_f";
						outputFile_kurtosis = outputFile_kurtosis + "_f";
						outputFile_sse = outputFile_sse + "_f";
					}

					outputFile_ccf = outputFile_ccf + ".csv";
					outputFile_kurtosis = outputFile_kurtosis + ".csv";
					outputFile_sse = outputFile_sse + ".csv";    

					// if lag >1 do ... else ... TODO:
					//lag = 7
					int l1 = l + 1;
					int l2 = l + 2;
					int l3 = l + 3;
					r.engine.eval("sorted_ccf_result" + eid + " <- t_ccf_result" + eid + "[order(-t_ccf_result" + eid + "[,"+ l1 +"], -t_ccf_result" + eid + "[,"+ l2 +"], -t_ccf_result" + eid + "[,"+ l3 +"]) , ]");        		        			        			
					//r.engine.eval("a = paste(\"home/tunguyen/correlation/\"atlantic_hurricane_season_en\", \"_\", \"u\", \"_d\", 14, \"_sm\", 1, \"_lag\", 7, \"_ccf.csv\", sep=\"\")");
					r.engine.eval("write.csv(sorted_ccf_result" + eid + ", \"" + outputFile_ccf + "\")");
					//r.engine.eval("write.csv(sorted_ccf_result" + eid + ", \"C:/Users/Kanhabua/Desktop/rfeature_results_08_12_13/correlation/" + main_event + "_en_" + type + "_d_" + d + "_sm_" + sm + "_lag_" + l + "_ccf.csv\")");


					//kurtosis	
					r.engine.eval("t_kurtosis_result" + eid + " <- t(kurtosis_result" + eid + ")");
					r.engine.eval("sorted_kurtosis_result" + eid + " <- t_kurtosis_result" + eid + " [order(-t_kurtosis_result" + eid + " [,1]),]");
					r.engine.eval("write.csv(sorted_kurtosis_result" + eid + ", \"" + outputFile_kurtosis + "\")");


					//sse
					r.engine.eval("t_sse_result" + eid + " <- t(sse_result" + eid + ")");
					r.engine.eval("sorted_sse_result" + eid + " <- t_sse_result" + eid + " [order(-t_sse_result" + eid + " [,1]),]");
					r.engine.eval("write.csv(sorted_sse_result" + eid + ", \"" + outputFile_sse + "\")");

					final long endTime1 = System.currentTimeMillis();

					System.out.println("Total execution time (for category): " + (endTime1 - startTime1) );

					// ## Clear time series variables ##
					r.removeTS(eid); 

					System.out.println("%%%%%%%  Start R-Features %%%%%%%");
				}
			}
		}	    		

		r.clearMainMatrix();
		r.endEngine();
		return;
	}

	private String getEventType(String category) {

		String type = "";

		if(category.equals("atlantic_hurricane_season")) {	type = "u"; }
		if(category.equals("aviation_accidents_and_incidents")) {	type = "u"; }
		if(category.equals("civil_wars")) {	type = "u"; }
		if(category.equals("conferences")) {	type = "p"; }
		if(category.equals("cricket_world_cup")) {	type = "p"; }
		if(category.equals("diplomatic_conferences")) {	type = "p"; }
		if(category.equals("earthquakes")) {	type = "u"; }
		if(category.equals("explosions")) {	type = "u"; }
		if(category.equals("fifa_world_cup")) {	type = "p"; }
		if(category.equals("floods")) {	type = "u"; }
		if(category.equals("industrial_disasters")) {	type = "u"; }
		if(category.equals("mass_murder_in")) {	type = "u"; }
		if(category.equals("olympic_games")) {	type = "p"; }
		if(category.equals("pacific_typhoon_season")) {	type = "u"; }
		if(category.equals("protests")) {	type = "u"; }
		if(category.equals("revolutions")) {	type = "u"; }
		if(category.equals("riots_and_civil_disorder")) {	type = "u"; }
		if(category.equals("suicide_bombings")) {	type = "u"; }
		if(category.equals("terrorist_incidents")) {	type = "u"; }
		if(category.equals("tsunamis")) {	type = "u"; }
		if(category.equals("uefa_champions_league")) {	type = "p"; }
		if(category.equals("volcanic_events")) {	type = "u"; }
		if(category.equals("wildfires")) {	type = "u"; }


		return type;
	}
}




