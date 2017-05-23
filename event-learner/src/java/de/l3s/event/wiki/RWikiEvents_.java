package l3s.de.event.wiki;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;

import l3s.de.event.wiki.RWikiEvents_.EventPeriod;
import l3s.de.rjava.TextConsole;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.joda.time.LocalDate;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

import scala.actors.threadpool.Arrays;

import de.l3s.tools.LocalDateBinner;

import au.com.bytecode.opencsv.CSVReader;

public class RWikiEvents_ {
	public Rengine engine;
	LocalDateBinner binner = new LocalDateBinner();



	public RWikiEvents_() {
		String[] rOptions = { "--vanilla" };
		engine = new Rengine(rOptions, false, new TextConsole());
		engine.eval("library(e1071)");
		engine.eval("library(lsa)");	
		engine.eval("library(TTR)");
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
		//engine.eval("path = paste(\"/home/kanhabua/projects/deduplication/data/WikiEvents/matrix/\", " + csv_file + ", sep=\"\")");
		//engine.eval("all_div <- read.csv(" + csv_file + ", header=T, sep='\t'");	
		engine.eval("all_div <- read.csv(\"/home/kanhabua/projects/deduplication/data/WikiEvents/matrix/all_events_en_transposed.txt\", header=T, sep=\"\t\")");
		engine.eval("source('/home/tunguyen/correlation/ccf.r')");
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
	}
	
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
    
	/**
	 * 
	 * @param args
	 * @throws ParseException
	 */
	public static void main (String[] args) throws ParseException {
		RWikiEvents_ r = new RWikiEvents_();
		r.init();

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
		
		// parse the command line arguments
		CommandLine line = parser.parse( options, args);
		//event type e.g., u
		String type = line.hasOption("t") ? line.getOptionValue("t") : "u";
		//event category
		String category = line.hasOption("c") ? line.getOptionValue("c") : "atlantic_hurricane_season";
		//path to event metadata file contains event date (csv)
	    String c_path = line.hasOption("cf") ? line.getOptionValue("cf") : "/atlantic_hurricane.csv";
	    //sliding window
	    String d_ = line.hasOption("d") ? line.getOptionValue("d") : "14";
	    int d = Integer.parseInt(d_);
	    //lag
	    String l_ = line.hasOption("l") ? line.getOptionValue("l") : "7";
	    int l = Integer.parseInt(l_);  
	    //sm
	    String sm_ = line.hasOption("sm") ? line.getOptionValue("sm") : "1";
	    double sm = Double.parseDouble(sm_);

		//load event metadata from csv file
		HashMap<String, EventPeriod> event_map = r.getEventMetadata_(c_path);
		Set<String> keyset = event_map.keySet();

		//triggering event list (after 2008)
		ArrayList<String> trg_events = new ArrayList<String>();
		LocalDate starting_point = r.binner.parseDate("2008-01-01");
		for(String expr : keyset){
			if(event_map.get(expr).startingDate.isAfter(starting_point)) {
				trg_events.add(expr);
			}
		}

		HashMap<String, Integer> event_idx = new HashMap<String, Integer>();
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
					event_idx.put(entry[2], Integer.parseInt(entry[0]));
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
			for (String s_event : event_idx.keySet()) {
				
				//check on plan type
				//date row of a matrix starts at row 2, row 1 is for labels
				main_event_sd =  event_map.get(main_event).startingDate;
				st = type.equals("u") ? main_event_sd.compareTo(starting_point) + 2 : main_event_sd.compareTo(starting_point) + 2 - d;
				et = event_map.get(main_event).endingDate.compareTo(starting_point) + 2 + 14;
				
				//st2 = event_map.get(s_event).startingDate.compareTo(starting_point) + 2 -14;
				//et2 = event_map.get(s_event).endingDate.compareTo(starting_point) + 2 + 14;
                System.out.println(main_event + " : " + event_idx.get(main_event));
                System.out.println(s_event + " : " + event_idx.get(s_event));
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
			r.engine.eval("write.csv(sorted_ccf_result" + eid + ", \"/home/tunguyen/correlation/" + main_event + "_en_" + type + "_d_" + d + "_sm_" + sm + "_lag_" + l + "_ccf.csv\")");
            
			
			
			//kurtosis	
			r.engine.eval("t_kurtosis_result" + eid + " <- t(kurtosis_result" + eid + ")");
			r.engine.eval("sorted_kurtosis_result" + eid + " <- t_kurtosis_result" + eid + " [order(-t_kurtosis_result" + eid + " [,1]),]");
			r.engine.eval("write.csv(sorted_kurtosis_result" + eid + ", \"/home/tunguyen/kurtosis/" + main_event + "_en_" + type + "_d_" + d + "_sm_" + sm + "_lag_" + l + "_kurtosis.csv\")");

			
			//sse

			r.engine.eval("t_sse_result" + eid + " <- t(sse_result" + eid + ")");
			r.engine.eval("sorted_sse_result" + eid + " <- t_sse_result" + eid + " [order(-t_sse_result" + eid + " [,1]),]");
			r.engine.eval("write.csv(sorted_sse_result" + eid + ", \"/home/tunguyen/sse/" + main_event + "_en_" + type + "_d_" + d + "_sm_" + sm + "_lag_" + l + "_sse.csv\")");
            eid++;
            
            
		}
	}
	
}




