package de.l3s.qac;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import l3s.de.event.features.RFeatures;

import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import de.l3s.tools.BinningStrategy;
import de.l3s.tools.LocalDateBinner;
import de.l3s.tools.LocalDateRange;
import edu.stanford.nlp.util.ArrayUtils;

public class Main {
	public static int[] AOLQueryFreq = {194270, 193166, 174561, 187500, 204781, 195542, 191979, 186704, 186467, 170881, 179476, 200703, 195598, 191157, 189206, 181466, 168326, 180857, 202299, 194153, 191562, 188006, 182499, 170712, 179924, 197948, 189091, 188752, 180729, 171966, 156886, 166246, 167825, 181209, 143107, 151390, 151511, 142058, 154214, 161267, 138061, 133851, 110293, 112925, 126409, 117365, 123859, 142332, 136364, 134911, 131774, 125122, 133909, 145749, 141998, 140122, 135832, 127689, 118098, 124517, 139862, 138850, 137941, 158194, 167449, 154824, 157955, 179577, 181748, 176669, 175288, 174788, 156827, 159445, 168565, 183859, 182491, 2006, 155202, 145605, 146466, 165878, 169559, 163630, 153144, 140620, 129912, 124740, 125839, 144681, 149247, 130920};
	public static int[] MSNQueryFreq = {319539, 323658, 311808, 296883, 282056, 155717, 165941, 325086, 318472, 310903, 325635, 295456, 181928, 179509, 360079, 362249, 347281, 340996, 302900, 167191, 178195, 351301, 338048, 347509, 326918, 283985, 155584, 152210, 181892, 334319, 332151};


	public Connection conn;
	public Main(){
		conn = connectDB3306();	
	}
	/**
	 * 
	 * @return
	 */
	public Connection connectDB3306() {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		Connection conn = null;
		try {
			String userName = "kanhabua";
			String password = "zFQ8XDpjMDcsy47K";
			String url = "jdbc:mysql://db.l3s.uni-hannover.de:3306/";
			conn = DriverManager.getConnection(url, userName, password);
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println ("Cannot connect to database server");
		}

		return conn;
	}

	public Map<String, Set<String>> parseL2R(List<String> lines) {
		Map<String, Set<String>> map = Maps.newHashMap();
		for (String line : lines) {
			line = line.substring(line.indexOf('#'), line.length());

			String[] data = line.split("\t");

			String candidate = data[1];
            System.out.println(data[1] + "aaaaaaa");
            
            candidate = candidate.substring(0, candidate.length() - 1);
			String query = data[2].replace("parInfo: q-knn_", "");
//			String query = data[2].replace("parInfo: rwrw_ranking_", "");
			
			query = query.replace("-fix", "");

			query = query.replaceAll("_", " ");

			String _date = data[data.length - 1];

			_date = _date.replace("_after", "");
			_date = _date.replace("_before", "");
			_date = _date.replace("_during", "");

			String[] dates = _date.split("_");

			String date = dates[1];
			Set<String> candidates;
			if (map.containsKey(query + "\t" + date)) {
				candidates = map.get(query + "\t" + date);
			} else {
				candidates = Sets.newHashSet();		
			}
			candidates.add(candidate);
			map.put(query + "\t" + date, candidates);

		}
		return map;
	}


	public static void main (String[] args) throws IOException, SQLException {

		Main m = new Main();
		System.out.println("start init");

		m.init(m.conn, "aol");
		System.out.println("done init");
		
		
//		System.out.println(Arrays.toString(m.getQueryLogTimeSeries("2006-03-14", "march madness bracket", "aol")));

		File inputFile = new File(args[0]);		
		if (inputFile.exists() && inputFile.isDirectory()) {

			File[] files = inputFile.listFiles();

			for (File f : files) {
				Map<String, Set<String>> map = m.parseL2R(Files.readLines(f, Charsets.UTF_8));
				String oPath = "/home/tunguyen/qac/";
				
				System.out.println(f.getName());
				System.out.println("----- Rank by MLE ------");


				Set<String> keys = map.keySet();
				BufferedWriter bw = Files.newWriter(new File(oPath + args[1] + "/" + f.getName() + "_MLE.txt"), Charsets.UTF_8);
				BufferedWriter bw2 = Files.newWriter(new File(oPath + args[1] + "/" + f.getName() + "_RMLE10.txt"), Charsets.UTF_8);
				
				BufferedWriter bw5 = Files.newWriter(new File(oPath + args[1] + "/" + f.getName() + "_RMLE3.txt"), Charsets.UTF_8);
				BufferedWriter bw3 = Files.newWriter(new File(oPath + args[1] + "/" + f.getName() + "_LastN.txt"), Charsets.UTF_8);
				BufferedWriter bw4 = Files.newWriter(new File(oPath + args[1] + "/" + f.getName() + "_NextN.txt"), Charsets.UTF_8);


				for (String key : keys) {
					String[] data = key.split("\t");
					String query = data[0];
					String date = data[1];

					Set<String> queries = map.get(key);
					System.out.println("----- Rank by MLE ------");

					Map<String, Double> ranked = m.rankByMLE(queries, date);
					
					for (String q : ranked.keySet()) {
						bw.append(q).append("\t").append(""+ranked.get(q));
						bw.newLine();
					}
					bw.append("######## " + query);	
					bw.newLine();

					System.out.println("----- Rank by Recent MLE ------");

					ranked = m.rankByRecentMLE(queries, date, 10);

					for (String q : ranked.keySet()) {
						bw2.append(q).append("\t").append(""+ranked.get(q));
						bw2.newLine();
					}

					bw2.append("######## " + query);	
					bw2.newLine();

					
					System.out.println("----- Rank by Recent MLE ------");

					ranked = m.rankByRecentMLE(queries, date, 3);

					for (String q : ranked.keySet()) {
						bw5.append(q).append("\t").append(""+ranked.get(q));
						bw5.newLine();
					}

					bw5.append("######## " + query);	
					bw5.newLine();
					
					System.out.println("----- Rank by Last N ------");

					int N = 20;
					System.out.println("N = " + N);
					ranked = m.rankByLastNQueries(query, date, N);

					for (String q : ranked.keySet()) {
						bw3.append(q).append("\t").append(""+ranked.get(q));
						bw3.newLine();
					}

					bw3.append("######## " + query);	
					bw3.newLine();
					System.out.println("----- Rank by Next N ------");
					ranked = m.rankByNextNQueries(query, date, N);

					for (String q : ranked.keySet()) {
						bw4.append(q).append("\t").append(""+ranked.get(q));
						bw4.newLine();
					}

					bw4.append("######## " + query);
					bw4.newLine();

				}
				bw.flush();
				bw.close();

				bw2.flush();
				bw2.close();

				bw3.flush();
				bw3.close();

				bw4.flush();
				bw4.close();

				m.r.endEngine();

				System.out.println("Done");

		
		}
	}

}

//public static void _main (String[] args) throws IOException, SQLException {
//
//	Main m = new Main();
//	System.out.println("start init");
//
//	m.init(m.conn, "aol");
//	System.out.println("done init");
//
//	File inputFile = new File(args[0]);		
//	if (inputFile.exists() && inputFile.isDirectory()) {
//
//		File[] files = inputFile.listFiles();
//
//		for (File f : files) {
//			String _path = f.getName();
//
//			System.out.println(_path);
//
//			String dates = _path.substring(_path.length() - "2006-02-28_2006-05-31_aol.dat".length(), _path.length());
//
//			_path = _path.replace("rwrw_ranking_", "");
//			_path = _path.replace("q-sim_", "");
//			_path = _path.replace("q-knn_", "");
//			String _query = null;
//			if (_path.contains("-fix")) {
//				_query = _path.substring(0, _path.indexOf("-fix"));
//			} else if (_path.contains("-vary")) {
//				_query = _path.substring(0, _path.indexOf("-vary"));
//			} else if (_path.contains("-burst")) {
//				_query = _path.substring(0, _path.indexOf("-burst"));
//			} 
//
//			String query = _query.replaceAll("_", " ");
//
//			System.out.println("Query: "  + query);
//
//			dates = dates.replaceAll("_aol.dat", "");
//			String[] _dates = dates.split("\\_");
//
//			Set<String> queries = Sets.newHashSet();
//			try {
//				List<String> lines = Files.readLines(f, Charsets.UTF_8);
//				if (lines.size() == 0) continue;
//				for (String line : lines) {
//					line = line.split("\\t")[1];
//					queries.add(line);
//				}
//
//
//
//				String fName = f.getName().replace(".dat", "");
//				String oPath = "/home/tunguyen/qac/";
//				System.out.println("----- Rank by MLE ------");
//
//				Set<String> ranked = m.rankByMLE(queries, _dates[1]);
//
//				BufferedWriter bw = Files.newWriter(new File(oPath + args[1] + "/" + fName +  "_MLE.txt"), Charsets.UTF_8);
//
//				for (String q : ranked) {
//					bw.append(q);
//					bw.newLine();
//				}
//				bw.flush();
//				bw.close();
//
//				System.out.println("----- Rank by Recent MLE ------");
//
//				ranked = m.rankByRecentMLE(queries, _dates[1], 10);
//
//				bw = Files.newWriter(new File(oPath + args[1] + "/" +fName + "_RMLE.txt"), Charsets.UTF_8);
//
//				for (String q : ranked) {
//					bw.append(q);
//					bw.newLine();
//				}
//				bw.flush();
//				bw.close();
//
//				System.out.println("----- Rank by Last N ------");
//
//				int N = 20;
//				System.out.println("N = " + N);
//				ranked = m.rankByLastNQueries(query, _dates[1], N);
//
//				bw = Files.newWriter(new File(oPath + args[1] + "/" +fName +  "_LastN.txt"), Charsets.UTF_8);
//
//				for (String q : ranked) {
//					bw.append(q);
//					bw.newLine();
//				}
//				bw.flush();
//				bw.close();
//
//				System.out.println("----- Rank by Next N ------");
//				m.rankByNextNQueries(query, _dates[1], N);
//
//				bw = Files.newWriter(new File(oPath + args[1] + "/" +fName + "_NextN.txt"), Charsets.UTF_8);
//
//				for (String q : ranked) {
//					bw.append(q);
//					bw.newLine();
//				}
//				bw.flush();
//				bw.close();
//
//				m.r.endEngine();
//
//				System.out.println("Done");
//
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//
//	}
//
//}



/*
 * Java method to sort Map in Java by value e.g. HashMap or Hashtable
 * throw NullPointerException if Map contains null values
 * It also sort values even if they are duplicates
 * Descending sorting
 */
public static <K extends Comparable,V extends Comparable> Map<K,V> sortByValues(Map<K,V> map){
	List<Map.Entry<K,V>> entries = new LinkedList<Map.Entry<K,V>>(map.entrySet());

	Collections.sort(entries, new Comparator<Map.Entry<K,V>>() {

		@Override
		public int compare(Entry<K, V> o1, Entry<K, V> o2) {
			return o2.getValue().compareTo(o1.getValue());
		}
	});

	//LinkedHashMap will keep the keys in the order they are inserted
	//which is currently sorted on natural ordering
	Map<K,V> sortedMap = new LinkedHashMap<K,V>();

	for(Map.Entry<K,V> entry: entries){
		sortedMap.put(entry.getKey(), entry.getValue());
	}

	return sortedMap;
}

/**
 * Normalize
 * @param query
 * @param date
 * @return
 * @throws IOException 
 * @throws SQLException 
 */
public double getAggregatedFreq(String date, String query) throws SQLException, IOException {
	double t_freq = 0;
	//get query time series 
	//dataset is AOL
	double[] ts = getQueryLogTimeSeries(date, query, "aol");
	if (ts == null) return 0;
	for (double t : ts) {
		t_freq += t;
	}
	System.out.println("aggregated: " + t_freq);
	return t_freq;
}

/**
 * Normalize
 * @param query
 * @param date
 * @param windowSize
 * @return
 * @throws IOException 
 * @throws SQLException 
 */
public double getRecentAggregatedFreq(String query, String date, int windowSize) throws SQLException, IOException {
	double t_freq = 0;
	//get query time series 
	//dataset is AOL
	double[] ts = getQueryLogTimeSeries(date, query, "aol");
	if (ts == null) return 0;
	if (ts.length > windowSize) {
		double[] _ts = Arrays.copyOfRange(ts, ts.length - windowSize, ts.length);
		System.out.println(_ts.length);
		for (double t  : _ts) {
			t_freq += t;
		}
		return t_freq;
	} else {
		for (double t  : ts) {
			t_freq += t;
		}
		return t_freq;
	}




}

/**
 * Most Popular Competion, aggregate all query freqs prior the 
 * hitting time. 
 * @param candidates
 * @param date
 */
public Map<String, Double> rankByMLE(Set<String> candidates, String date) {
	Map<String, Double> map = Maps.newHashMapWithExpectedSize(candidates.size());
	for (String c : candidates) {
		System.out.println("candidate: " + c);
		try {
			map.put(c, getAggregatedFreq(date, c));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	Map<String, Double> sortedMap = sortByValues(map);
	return sortedMap;

}

/**
 * Aggregate only last-N days of query evidence
 * N = 2,4,7,14,28
 * @param candidates
 * @param date
 * @param windowSize
 */
public Map<String, Double> rankByRecentMLE(Set<String> candidates, String date, int windowSize) {
	Map<String, Double> map = Maps.newHashMapWithExpectedSize(candidates.size());
	for (String c : candidates) {
		try {
			map.put(c, getRecentAggregatedFreq(c, date, windowSize));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	Map<String, Double> sortedMap = sortByValues(map);
	return sortedMap;

}

/**
 * get last N queries according to 
 * hitting time
 * @param prefix
 * @param date
 * @param N
 * @return
 */
public Set<String> getLastNQueries (Set<String> candidates, String prefix, String date, int N) {		
	String sql = "SELECT Query FROM TemporalIR.aol_query_url WHERE Query LIKE ? AND DATE(Time) = ?";
	try {
		PreparedStatement stm = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);

		stm.setString(1, "%"+prefix+"%");
		stm.setString(2, date);

		System.out.println(stm.toString());

		ResultSet rs = stm.executeQuery();
		while (rs.next()) {
			candidates.add(rs.getString("Query"));
		}
		stm.close();
		rs.close();
		if (candidates.size() < N) {
			date = binner.parseDate(date).minusDays(1).toString();

			if (date.equals("2006-02-28")) return candidates;
			getLastNQueries(candidates, prefix, date, N);
		}
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}



	return candidates;
}

public Map<String, Double> rankByLastNQueries(String prefix, String date, int N) {
	Set<String> candidates = Sets.newHashSetWithExpectedSize(N);
	prefix = prefix.replace("2006 ", "");
	String[] p = prefix.split("\\s");

	//get only the first two words
	prefix = (p.length > 1) ? p[0] + " " + p[1] : p[0];
	return rankByMLE(getLastNQueries(candidates, prefix, date, N), date);
}

public int[] getQueryTS(String query, int windowSize) {
	int[] ts = null;



	return ts;
}

RFeatures r = new RFeatures();

/**
 * Ranks completion suggestions based on their predicted
 * popularity distribution at hitting time
 * using time series prediction technique
 * 
 * the time series granularity need to be specified as input
 * (hours, days etc)
 * @param candidates
 * @return
 * @throws IOException 
 * @throws SQLException 
 */
public Map<String, Double> rankByNextNQueries(String prefix, String date, int N) throws SQLException, IOException {
	Map<String, Double> map = Maps.newHashMapWithExpectedSize(N);
	Set<String> candidates = Sets.newHashSetWithExpectedSize(N);
	prefix = prefix.replace("2006 ", "");
	String[] p = prefix.split("\\s");

	//get only the first two words
	prefix = (p.length > 1) ? p[0] + " " + p[1] : p[0];
	getLastNQueries(candidates, prefix, date, N);
	double[] ts;
	//int windowSize = 10;
	for (String c : candidates) {
		//ts = getQueryTS(c, windowSize);
		ts = getQueryLogTimeSeries(date, c, "aol");
		double predicted = r.getPredictedValue(ts);
		System.out.println(Arrays.toString(ts));
		System.out.println("predicted: " + predicted);
		map.put(c, predicted);
	}
	Map<String, Double> sortedMap = sortByValues(map);
	return sortedMap;

}
BinningStrategy<LocalDate> binner = new LocalDateBinner();
public double[] getQueryLogTimeSeries(String qTime, String query, String dataset) throws SQLException, IOException {

	HashMap<String,Integer> clusterQueryTs = new HashMap<String,Integer>();

	ArrayList<String> tsKeySet = new ArrayList<String>();
	String start_date = (dataset.equals("aol")) ? "2006-03-01" : "2006-05-01";

	LocalDateRange dateRange = new LocalDateRange(binner.parseDate(start_date),
			binner.parseDate(qTime));
	
	System.out.println("Date: " + qTime + " " + query + "aaaa");

	for (LocalDate date : dateRange) {
		String sqlDate = date.toString();
		tsKeySet.add(sqlDate);
		clusterQueryTs.put(sqlDate, 0);
	}

	ArrayList<QueryDailyFreq> qdfList = queryDailyFreqMap.get(query);
	if (qdfList == null) {
		return null;
	}
	for (QueryDailyFreq qdf: qdfList) {
		if (clusterQueryTs.containsKey(qdf.date)) {
			clusterQueryTs.put(qdf.date, clusterQueryTs.get(qdf.date) + qdf.freq);
		} 
	}

	double[] q_ts = new double[tsKeySet.size()];
	

	//normalize
	int i = 0;
	for (String sqlD: tsKeySet) {
		q_ts[i] = (double) clusterQueryTs.get(sqlD) * 1000 / AOLQueryFreq[i];
		i++;
	}

	return q_ts;

} 
HashMap<String, ArrayList<QueryDailyFreq>> queryDailyFreqMap = new HashMap<String, ArrayList<QueryDailyFreq>>();

public void init(Connection conn, String dataset) throws SQLException {
	String q2 = "SELECT * FROM TemporalIR." + dataset + "qdf";
	Statement s = conn.createStatement();
	s.executeQuery(q2);
	ResultSet rs = s.getResultSet();
	while (rs.next()) {
		String query = rs.getString("Query");
		String date = rs.getString("Date");
		int freq = rs.getInt("Freq");
		ArrayList<QueryDailyFreq> qdfList;
		if (queryDailyFreqMap.containsKey(query)){
			qdfList = queryDailyFreqMap.get(query);
		}else{
			qdfList = new ArrayList<QueryDailyFreq>();
		}
		qdfList.add(new QueryDailyFreq(query, date, freq));
		queryDailyFreqMap.put(query, qdfList);
	}
	rs.close();
	s.close();

	System.out.println(queryDailyFreqMap.size());
}

public class QueryDailyFreq{
	public String query;
	public String date;
	public int freq;
	public QueryDailyFreq(String query, String date, int freq){
		this.query = query;
		this.date = date;
		this.freq = freq;
	}
}

String[] fixTimePar_msn = {"2006-05-07", "2006-05-14", "2006-05-21", "2006-05-31"};
String[] fixTimePar_aol = {"2006-03-07", "2006-03-14", "2006-03-21", "2006-03-31", 
		"2006-04-07", "2006-04-14", "2006-04-21", "2006-04-30", 
		"2006-05-07", "2006-05-14", "2006-05-21", "2006-05-31"};





}
