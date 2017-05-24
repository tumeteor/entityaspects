package l3s.de.event.features;

import au.com.bytecode.opencsv.CSVReader;
import de.l3s.timesynonym.burst.NYTBurstDetector;
import de.l3s.tools.*;
import org.apache.commons.lang.ArrayUtils;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.joda.time.DateMidnight;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.actors.threadpool.Arrays;

import java.io.*;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;

public class EventDistinctFeatures {
    public static final String MSN = "msn";
    public static final String AOL = "aol";
    public static final String IMP = "imp";
    public static final String EXP = "expl";
    public static final int BREAKING = 1;
    public static final int ONGOING = 2;
    public static final int ANTICIPATED = 3;
    public static final int MEME = 4;
    public static final int COMMEMORATIVE = 5;
    public static final String TRENDING = "trending";
    public static final String GENERAL = "general";
    public Connection conn;
    String[] fixTimePar_msn = {"2006-05-07", "2006-05-14", "2006-05-21", "2006-05-31"};
    String[] fixTimePar_aol = {"2006-03-07", "2006-03-14", "2006-03-21", "2006-03-31",
            "2006-04-07", "2006-04-14", "2006-04-21", "2006-04-30",
            "2006-05-07", "2006-05-14", "2006-05-21", "2006-05-31"};
    BinningStrategy<LocalDate> binner = new LocalDateBinner();

    public EventDistinctFeatures() {
        conn = connectDB3306();
    }

    public static void main(String[] args) {
        EventDistinctFeatures edf = new EventDistinctFeatures();
        edf.computeFeatures_MainClass();
//		edf.loadAnnotatedData();
    }

    /**
     * Create a new list which contains the specified number of elements from the source list, in a
     * random order but without repetitions.
     *
     * @param sourceList    the list from which to extract the elements.
     * @param itemsToSelect the number of items to select
     * @param random        the random number generator to use
     * @return a new list   containg the randomly selected elements
     */
    public static int[] chooseRandomly(int[] sourceArray, int itemsToSelect, Random random) {
        int sourceSize = sourceArray.length;
        // Generate an array representing the element to select from 0... number of available
        // elements after previous elements have been selected.
        int[] selections = new int[itemsToSelect];
        // Simultaneously use the select indices table to generate the new result array
        int[] resultArray = new int[itemsToSelect];
        for (int ind = 0; ind < itemsToSelect; ind++) {
            // An element from the elements *not yet chosen* is selected
            int selection = random.nextInt(sourceSize - ind);
            selections[ind] = selection;
            // Store original selection in the original range 0.. number of available elements
            // This selection is converted into actual array space by iterating through the elements
            // already chosen.
            for (int scanIdx = ind - 1; scanIdx >= 0; scanIdx--) {
                if (selection >= selections[scanIdx]) {
                    selection++;
                }
            }
            // When the first selected element record is reached all selections are in the range
            // 0.. number of available elements, and free of collisions with previous entries.
            // Write the actual array entry to the results
            resultArray[ind] = sourceArray[selection];
        }
        return resultArray;
    }

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
            System.err.println("Cannot connect to database server");
        }

        return conn;
    }

    public void computeFeatures_MainClass() {
        try {
            BufferedWriter out0 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_expl_3labels_par0307.arff")));
            BufferedWriter out1 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_expl_3labels_par0314.arff")));
            BufferedWriter out2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_expl_3labels_par0321.arff")));
            BufferedWriter out3 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_expl_3labels_par0331.arff")));
            BufferedWriter out4 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_expl_3labels_par0407.arff")));
            BufferedWriter out5 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_expl_3labels_par0414.arff")));
            BufferedWriter out6 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_expl_3labels_par0421.arff")));
            BufferedWriter out7 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_expl_3labels_par0430.arff")));
            BufferedWriter out8 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_expl_3labels_par0507.arff")));
            BufferedWriter out9 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_expl_3labels_par0514.arff")));
            BufferedWriter out10 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_expl_3labels_par0521.arff")));
            BufferedWriter out11 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_expl_3labels_par0531.arff")));
            BufferedWriter[] bws = new BufferedWriter[12];

            bws[0] = out0;
            bws[1] = out1;
            bws[2] = out2;
            bws[3] = out3;
            bws[4] = out4;
            bws[5] = out5;
            bws[6] = out6;
            bws[7] = out7;
            bws[8] = out8;
            bws[9] = out9;
            bws[10] = out10;
            bws[11] = out11;

            for (BufferedWriter out : bws) {
                out.write("@relation event_prediction");
                out.newLine();
                out.write("@attribute qid numeric");
                out.newLine();
                out.write("@attribute long_span_acf numeric");
                out.newLine();
                out.write("@attribute long_span_seasonal numeric");
                out.newLine();
                out.write("@attribute long_span_KL_PT numeric");
                out.newLine();
                out.write("@attribute long_span_kurtosis numeric");
                out.newLine();
                out.write("@attribute short_span_acf numeric");
                out.newLine();
                out.write("@attribute short_span_seasonal numeric");
                out.newLine();
                out.write("@attribute short_span_kurtosis numeric");
                out.newLine();
                out.write("@attribute prediction_sse numeric");
                out.newLine();
                out.write("@attribute t_scope numeric");
                out.newLine();
                out.write("@attribute t_level numeric");
                out.newLine();
                out.write("@attribute maxFreq numeric");
                out.newLine();
                out.write("@attribute avgFreq numeric");
                out.newLine();
                out.write("@attribute burstWeight numeric");  //nearest one
                out.newLine();
                out.write("@attribute burstLength numeric");  //nearest one
                out.newLine();
                out.write("@attribute noOfBursts numeric");
                out.newLine();
                out.write("@attribute isTempEx numeric"); //[0-1], 1 is true
                out.newLine();
                out.write("@attribute CEshort numeric");
                out.newLine();
                out.write("@attribute CElong numeric");
                out.newLine();
                out.write("@attribute CEper numeric");
                out.newLine();
                out.write("@attribute isLoc numeric");
                out.newLine();
                out.write("@attribute isPer numeric");
                out.newLine();
                out.write("@attribute isOrg numeric");
                out.newLine();
                out.write("@attribute no_queries numeric");
                out.newLine();
                out.write("@attribute sumCFreq numeric");
                out.newLine();
                out.write("@attribute avgCFreq numeric");
                out.newLine();
                out.write("@attribute maxCFreq numeric");
                out.newLine();
                out.write("@attribute burstDistM numeric");
                out.newLine();
                out.write("@attribute burstDistL numeric");
                out.newLine();
                //out.write("@attribute class_LMTU {time, nontime}");
                //out.write("@attribute class {recurrent ,planned_nonrecurrent, unplanned_nonrecurrent, non-event}");
                out.write("@attribute class {general, trending, non-event}");
                out.newLine();
                out.write("@data");
                out.newLine();
            }

            RFeatures rFeatures = new RFeatures();
            int index = 0;

            //aol_impl or aol_expl or msn_impl or msn_expl

            CSVReader nyt_reader = new CSVReader(new InputStreamReader(getClass().getResourceAsStream("/training_data/aol_expl2.csv")), '\t');
            CSVReader querylog_reader = new CSVReader(new InputStreamReader(getClass().getResourceAsStream("/training_data/aol_expl_ts.csv")), '\t');
            String[] entry;

            //store querylog timeseries csv file in hashmap
            HashMap<String, ArrayList<int[]>> querylog_map = new HashMap<String, ArrayList<int[]>>();
            while ((entry = querylog_reader.readNext()) != null) {
                String[] ql_ts_array = entry[3].substring(1, entry[3].length() - 1).split("\\,");
                int[] ql_ts = new int[ql_ts_array.length];
                for (int i = 0; i < ql_ts_array.length; i++) {
                    ql_ts[i] = Integer.parseInt(ql_ts_array[i].trim());
                }
                ArrayList<int[]> tsList;
                if (querylog_map.containsKey(entry[1])) {
                    tsList = querylog_map.get(entry[1]);
                } else {
                    tsList = new ArrayList<int[]>();
                }
                tsList.add(ql_ts);
                querylog_map.put(entry[1], tsList);
            }
            ArrayList<String> tempExList = null;
            try {
                tempExList = EventFeatures.tempExList();
            } catch (URISyntaxException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            while ((entry = nyt_reader.readNext()) != null) {
                String query = entry[0];

                double kl_score = Double.parseDouble(entry[2]);
                //clean up explicit queries
                //for monthly granularity, freq = 12
                String[] nyt_ts_array = entry[1].substring(1, entry[1].length() - 1).split("\\,");
                int[] nyt_ts = new int[nyt_ts_array.length];
                for (int i = 0; i < nyt_ts_array.length; i++) {
                    nyt_ts[i] = Integer.parseInt(nyt_ts_array[i].trim());
                }

                //autocorrelation
                double[] acf = rFeatures.computeAutoCorrel(12, nyt_ts);
                //seasonality value for NYT collection
                double long_span_seasonal = rFeatures.decomposeTSSeasonalityfromNYTwithCosineSim(nyt_ts);
                //kurtosis
                double kurtosis_ts = rFeatures.getKurtosisTimeSeries(nyt_ts);
                //isTempEx
                int isTempEx = (tempExList.contains(query)) ? 1 : 0;
                int[] ne = null;
                try {
                    //extract NEs of first-letter capitalized queries
                    PreparedStatement stm = conn.prepareStatement("SELECT Cap_query_entities FROM TemporalIR." + AOL + "_query WHERE Query = ?"
                            , PreparedStatement.RETURN_GENERATED_KEYS);
                    ne = queryNE(query, stm);
                    stm.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                if (!querylog_map.containsKey(query)) {
                    System.out.println(query);
                    continue;
                }
                ArrayList<int[]> tsList = querylog_map.get(query);
                for (int[] querylog_ts : tsList) {
                    int sum_qts = 0;
                    for (int i : querylog_ts) {
                        sum_qts += i;
                    }
                    //ignore queries that are not issued before hitting time
                    if (sum_qts == 0) continue;
                    String label;
                    String label_ = "";
                    if (entry[3].equals("non-event")) label = "non-event";
                    else {
                        String eventT = entry[3].trim();
                        DateMidnight sTime;
                        DateMidnight eTime;
                        DateTimeFormatter dateFormat = DateTimeFormat
                                .forPattern("yyyyMMdd");
                        System.out.println(query);
                        if (eventT.length() > 8) {
                            String[] seT = eventT.split("\\-");
                            sTime = binner.parseDate(seT[0], dateFormat).toDateMidnight();
                            eTime = binner.parseDate(seT[1], dateFormat).toDateMidnight();
                        } else {
                            sTime = eTime = binner.parseDate(eventT, dateFormat).toDateMidnight();
                        }
                        int type;
                        if (entry[4].contains("1")) type = BREAKING;
                        else if (entry[4].contains("2")) type = ONGOING;
                        else if (entry[4].contains("3")) type = ANTICIPATED;
                        else if (entry[4].contains("4")) type = MEME;
                        else type = COMMEMORATIVE;
                        label_ = entry[4] + " - " + entry[5];
                        //dynamic label
                        label = dynamicLabeling(sTime, eTime, binner.parseDate(getDateFromTS("aol", querylog_ts.length)).toDateMidnight(),
                                type);
                    }

                    if (label == TRENDING) System.out.println(query + " " + TRENDING);


                    //autocorrelation
                    double[] short_acf = rFeatures.computeAutoCorrel(12, querylog_ts);
                    //seasonality value for QueryLog collection
                    double short_span_seasonal;
                    //Holt-Winters prediction SSE
                    double prediction_sse;
                    //trending level
                    double t_level;
                    //tredning slope
                    double t_slope;
                    //for querylog timeseries, set Freq = 3
                    //max burst weight
                    double burst_weight = 0.0;
                    //max burst_length;
                    double burst_length = 0.0;


                    double distW = Double.NaN;
                    double distL = Double.NaN;
                    double[] dist = null;
                    int[] qstats = null;
                    //number of bursts
                    int no_of_bursts = 0;
                    int sum_ts_ql = 0;
                    for (int i : querylog_ts) {
                        sum_ts_ql += i;
                    }
                    if (querylog_ts.length < 6 || sum_ts_ql == 0) {
                        short_span_seasonal = Double.NaN;
                        prediction_sse = Double.NaN;
                        t_level = Double.NaN;
                        t_slope = Double.NaN;
                    } else {
                        short_span_seasonal = rFeatures.decomposeTSSeasonalityfromQLwithCosineSim(querylog_ts);
                        double[] holt_winter = rFeatures.exponentialSmoothingQLTimeseries(querylog_ts);
                        prediction_sse = holt_winter[0];
                        t_level = holt_winter[1];
                        t_slope = holt_winter[2];
                        QueryLogBurstDetector burstDetector;
                        SimpleBurstCollector<String, LocalDate> collector = null;

                        if (querylog_ts.length <= QueryLogBurstDetector.AOLQueryFreq.length) {
                            burstDetector = new QueryLogBurstDetector(conn, "2006-03-01", querylog_ts.length, "aol");
                            collector = burstDetector.detectTermBursts(query, querylog_ts);
                        }

                        if (collector == null || collector.getBursts().size() == 0) {
                            no_of_bursts = 0;
                            burst_weight = Double.NaN;
                            burst_length = Double.NaN;
                        } else {
                            no_of_bursts = collector.getBursts().size();

                            // distance to max burst and last burst
                            System.out.println(query + " querylog length: " + querylog_ts.length);
                            dist = getBurstDistance(collector, AOL, querylog_ts.length);
                            distW = dist[0];
                            distL = dist[1];
                            burst_weight = dist[2];
                            burst_length = dist[3];
                            System.out.println("Burst Stats: " + Arrays.toString(dist));
                        }

                    }
                    //kurtosis
                    double short_kurtosis_ts = rFeatures.getKurtosisTimeSeries(querylog_ts);
                    int max = 0;
                    int sum = 0;
                    for (int ts_element : querylog_ts) {
                        sum += ts_element;
                        if (max < ts_element) max = ts_element;
                    }

                    int sum_ts = 0;
                    for (int i : nyt_ts) {
                        sum_ts += i;
                    }

                    double[] CE = null;
                    try {
                        CE = getClickEntropyPercentage(query, querylog_ts.length, AOL);
                        System.out.println("CE: " + CE[0] + " " + CE[1]);
                    } catch (SQLException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    try {
                        // cluster stats
                        qstats = queryClusterStats(query, querylog_ts.length, AOL);
                        System.out.println("Cluster aggregate: " + qstats[0] + " " + qstats[1] + " " + qstats[2]);

                    } catch (SQLException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    System.out.println("Cluster aggregate: " + qstats[0] + " " + qstats[1] + " " + qstats[2] + " " + query);
                    //click entropy
                    //						double CE = 0.0d;
                    //						try {
                    //							CE = getClickEntropyPercentage(query, querylog_ts.length, AOL);
                    //						} catch (SQLException e) {
                    //							// TODO Auto-generated catch block
                    //							e.printStackTrace();
                    //						}
                    if (sum_ts == 0) {
                        if (querylog_ts.length == 7) {
                            out0.write(index + ",?,?,?,?," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out0.newLine();
                            out0.flush();
                        } else if (querylog_ts.length == 14) {
                            out1.write(index + ",?,?,?,?," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out1.newLine();
                            out1.flush();
                        } else if (querylog_ts.length == 21) {
                            out2.write(index + ",?,?,?,?," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out2.newLine();
                            out2.flush();
                        } else if (querylog_ts.length == 31) {
                            out3.write(index + ",?,?,?,?," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out3.newLine();
                            out3.flush();
                        } else if (querylog_ts.length == 38) {
                            out4.write(index + ",?,?,?,?," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out4.newLine();
                            out4.flush();
                        } else if (querylog_ts.length == 45) {
                            out5.write(index + ",?,?,?,?," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out5.newLine();
                            out5.flush();
                        } else if (querylog_ts.length == 52) {
                            out6.write(index + ",?,?,?,?," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out6.newLine();
                            out6.flush();
                        } else if (querylog_ts.length == 61) {
                            out7.write(index + ",?,?,?,?," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out7.newLine();
                            out7.flush();
                        } else if (querylog_ts.length == 68) {
                            out8.write(index + ",?,?,?,?," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out8.newLine();
                            out8.flush();
                        } else if (querylog_ts.length == 75) {
                            out9.write(index + ",?,?,?,?," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out9.newLine();
                            out9.flush();
                        } else if (querylog_ts.length == 82) {
                            out10.write(index + ",?,?,?,?," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out10.newLine();
                            out10.flush();
                        } else if (querylog_ts.length == 92) {
                            out11.write(index + ",?,?,?,?," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out11.newLine();
                            out11.flush();
                        } else
                            System.out.println(querylog_ts.length + "-------------------------------------------------------------------------------------------------------");
                    } else {
                        if (querylog_ts.length == 7) {
                            out0.write(index + "," + acf[1] + "," + long_span_seasonal + "," + kl_score + "," + kurtosis_ts + "," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out0.newLine();
                            out0.flush();
                        } else if (querylog_ts.length == 14) {
                            out1.write(index + "," + acf[1] + "," + long_span_seasonal + "," + kl_score + "," + kurtosis_ts + "," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out1.newLine();
                            out1.flush();
                        } else if (querylog_ts.length == 21) {
                            out2.write(index + "," + acf[1] + "," + long_span_seasonal + "," + kl_score + "," + kurtosis_ts + "," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out2.newLine();
                            out2.flush();
                        } else if (querylog_ts.length == 31) {
                            out3.write(index + "," + acf[1] + "," + long_span_seasonal + "," + kl_score + "," + kurtosis_ts + "," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out3.newLine();
                            out3.flush();
                        } else if (querylog_ts.length == 38) {
                            out4.write(index + "," + acf[1] + "," + long_span_seasonal + "," + kl_score + "," + kurtosis_ts + "," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out4.newLine();
                            out4.flush();
                        } else if (querylog_ts.length == 45) {
                            out5.write(index + "," + acf[1] + "," + long_span_seasonal + "," + kl_score + "," + kurtosis_ts + "," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out5.newLine();
                            out5.flush();
                        } else if (querylog_ts.length == 52) {
                            out6.write(index + "," + acf[1] + "," + long_span_seasonal + "," + kl_score + "," + kurtosis_ts + "," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out6.newLine();
                            out6.flush();
                        } else if (querylog_ts.length == 61) {
                            out7.write(index + "," + acf[1] + "," + long_span_seasonal + "," + kl_score + "," + kurtosis_ts + "," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out7.newLine();
                            out7.flush();
                        } else if (querylog_ts.length == 68) {
                            out8.write(index + "," + acf[1] + "," + long_span_seasonal + "," + kl_score + "," + kurtosis_ts + "," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out8.newLine();
                            out8.flush();
                        } else if (querylog_ts.length == 75) {
                            out9.write(index + "," + acf[1] + "," + long_span_seasonal + "," + kl_score + "," + kurtosis_ts + "," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out9.newLine();
                            out9.flush();
                        } else if (querylog_ts.length == 82) {
                            out10.write(index + "," + acf[1] + "," + long_span_seasonal + "," + kl_score + "," + kurtosis_ts + "," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out10.newLine();
                            out10.flush();
                        } else if (querylog_ts.length == 92) {
                            out11.write(index + "," + acf[1] + "," + long_span_seasonal + "," + kl_score + "," + kurtosis_ts + "," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + t_level + "," + t_slope + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + CE[0] + "," + CE[1] + "," + CE[2] + "," + ne[0] + "," + ne[1] + "," + ne[2] + "," + qstats[0] + "," + qstats[1] + "," + qstats[2] + "," + qstats[3] + "," + distW + "," + distL + "," + label + "%" + query + " " + label_);
                            out11.newLine();
                            out11.flush();
                        } else
                            System.out.println(querylog_ts.length + "-------------------------------------------------------------------------------------------------------");
                    }
                    index++;
                }
            }


            for (BufferedWriter out : bws) {
                out.close();
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void loadAnnotatedData() {
        CSVReader elabeled_reader = new CSVReader(new InputStreamReader(getClass().getResourceAsStream("/event-related_queries_labeled_manual_final_030113.csv")), ',');
        String[] entry;
        HashMap<String, String[]> aol_impl_map = new HashMap<String, String[]>();
        HashMap<String, String[]> aol_expl_map = new HashMap<String, String[]>();
        HashMap<String, String[]> msn_impl_map = new HashMap<String, String[]>();
        HashMap<String, String[]> msn_expl_map = new HashMap<String, String[]>();
        try {
            //differentiate and load event_labeled data into hashmap
            while ((entry = elabeled_reader.readNext()) != null) {
                //				if(entry[1].equals("MSN_expl")) {
                //					msn_expl_map.put(entry[0], entry);
                //				}
                //				else if(entry[1].equals("MSN_impl")) {
                //					msn_impl_map.put(entry[0], entry);
                //				}
                if (entry[1].equals("AOL_expl")) {
                    aol_expl_map.put(entry[0], entry);
                } else if (entry[1].equals("AOL_impl")) {
                    aol_impl_map.put(entry[0], entry);
                }
            }
            System.out.println("AOL impl: " + aol_impl_map.size());
            System.out.println("AOL expl: " + aol_expl_map.size());
            //			System.out.println("MSN impl: " + msn_impl_map.size());
            //			System.out.println("MSN expl: " + msn_expl_map.size());
            //randomly select, differentiate and load non_event_labeled data into hashmap
            InputStream aol_impl_cluster = getClass().getResourceAsStream("/training_data/event_cluster_partitioned_aol_grouped_Nattiya.xlsx");
            //			InputStream msn_impl_cluster = getClass().getResourceAsStream("/training_data/event_cluster_partitioned_msn_grouped_Nattiya.xlsx");
            InputStream aol_expl_cluster = getClass().getResourceAsStream("/training_data/explicit_queries_global_freq_aol_Nattiya.xlsx");
            //			InputStream msn_expl_cluster = getClass().getResourceAsStream("/training_data/explicit_queries_global_freq_msn_Nattiya.xlsx");
            Workbook aol_impl_wb;
            Workbook msn_impl_wb;
            Workbook aol_expl_wb;
            Workbook msn_expl_wb;
            try {
                aol_impl_wb = WorkbookFactory.create(aol_impl_cluster);
                //				msn_impl_wb = WorkbookFactory.create(msn_impl_cluster);
                aol_expl_wb = WorkbookFactory.create(aol_expl_cluster);
                //				msn_expl_wb = WorkbookFactory.create(msn_expl_cluster);
                Sheet aol_impl_sheet = aol_impl_wb.getSheetAt(0);
                Sheet aol_expl_sheet = aol_expl_wb.getSheetAt(0);
                //				Sheet msn_impl_sheet = msn_impl_wb.getSheetAt(0);
                //				Sheet msn_expl_sheet = msn_expl_wb.getSheetAt(0);
                Random rand = new Random(100000);

                Iterator<Row> aol_impl_rows = aol_impl_sheet.iterator();
                ArrayList<Integer> aol_impl_rowlist = new ArrayList<Integer>();
                while (aol_impl_rows.hasNext()) {
                    aol_impl_rowlist.add(aol_impl_rows.next().getRowNum());
                }

                //for AOL implicit
                //473 - 305

                System.out.println("AOL impl # of rows: " + aol_impl_rowlist.size());
                int[] random_aol_impl = chooseRandomly(ArrayUtils.toPrimitive(aol_impl_rowlist.toArray(new Integer[aol_impl_rowlist.size()])), 20000, rand);
                System.out.println("size: " + random_aol_impl.length);
                String[] aol_row = new String[1];
                int j = 0;
                for (int i = 0; i < random_aol_impl.length; i++) {
                    Row aol_impl_row = aol_impl_sheet.getRow(random_aol_impl[i]);
                    if (aol_impl_row.getCell(0) == null || aol_impl_row.getCell(1) == null) continue;
                    String aol_impl_query = aol_impl_row.getCell(0).getStringCellValue();
                    String aol_impl_label = aol_impl_row.getCell(1).getStringCellValue();
                    aol_row[0] = aol_impl_label;
                    //ignore some queries for concrete labeling
                    if (aol_impl_label.contains("Not event") && !aol_impl_query.contains("final fantasy")
                            && !aol_impl_query.contains("ticket")) {
                        aol_impl_map.put(aol_impl_query, aol_row);
                        j++;
                    }
                    if (j == 350) break;
                }

                System.out.println("non event: " + j);

                System.out.println("total queries: " + aol_impl_map.size());

                Iterator<Row> aol_expl_rows = aol_expl_sheet.iterator();
                ArrayList<Integer> aol_expl_rowlist = new ArrayList<Integer>();
                while (aol_expl_rows.hasNext()) {
                    aol_expl_rowlist.add(aol_expl_rows.next().getRowNum());
                }
                //for AOL explicit
                //499 - 256
                int[] random_aol_expl = chooseRandomly(ArrayUtils.toPrimitive(aol_expl_rowlist.toArray(new Integer[aol_expl_rowlist.size()])), 1000, rand);
                j = 0;
                for (int i = 0; i < random_aol_expl.length; i++) {
                    Row aol_expl_row = aol_expl_sheet.getRow(random_aol_expl[i]);
                    if (aol_expl_row.getCell(2) == null || aol_expl_row.getCell(3) == null) continue;
                    String aol_expl_query = null;
                    try {
                        aol_expl_query = aol_expl_row.getCell(2).getStringCellValue();
                    } catch (IllegalStateException ise) {
                        continue;
                    }
                    String aol_expl_label = aol_expl_row.getCell(3).getStringCellValue();
                    aol_row[0] = aol_expl_label;
                    if (aol_expl_label.contains("Not event")) {
                        aol_expl_map.put(aol_expl_query, aol_row);
                        j++;
                    }
                    if (j == 300) break;
                }

                System.out.println("total queries: " + aol_expl_map.size());

                /**
                 Iterator<Row> msn_impl_rows = msn_impl_sheet.iterator();
                 ArrayList<Integer> msn_impl_rowlist = new ArrayList<Integer>();
                 while(msn_impl_rows.hasNext()){
                 msn_impl_rowlist.add(msn_impl_rows.next().getRowNum());
                 }
                 //for MSN implicit
                 //158 -266
                 int[] random_msn_impl = chooseRandomly(ArrayUtils.toPrimitive(msn_impl_rowlist.toArray(new Integer[msn_impl_rowlist.size()])), 200, rand);
                 String[] msn_row = new String[1];
                 for(int i = 0; i < random_msn_impl.length; i++){
                 Row msn_impl_row = msn_impl_sheet.getRow(random_msn_impl[i]);
                 if(msn_impl_row.getCell(0) == null || msn_impl_row.getCell(1) == null) continue;
                 String msn_impl_query = msn_impl_row.getCell(0).getStringCellValue();
                 String msn_impl_label = msn_impl_row.getCell(1).getStringCellValue();
                 System.out.println("MSN impl: " + msn_impl_query + " " + msn_impl_label);
                 msn_row[0] = msn_impl_label;
                 if(msn_impl_label.contains("Not event")){
                 msn_impl_map.put(msn_impl_query,msn_row);
                 }
                 }
                 Iterator<Row> msn_expl_rows = msn_expl_sheet.iterator();
                 ArrayList<Integer> msn_expl_rowlist = new ArrayList<Integer>();
                 while(msn_expl_rows.hasNext()){
                 msn_expl_rowlist.add(msn_expl_rows.next().getRowNum());
                 }
                 //for MSN explicit
                 //159 - 227
                 int[] random_msn_expl = chooseRandomly(ArrayUtils.toPrimitive(msn_expl_rowlist.toArray(new Integer[msn_expl_rowlist.size()])), 300, rand);
                 for(int i = 0; i < random_msn_expl.length; i++){
                 Row msn_expl_row = msn_expl_sheet.getRow(random_msn_expl[i]);
                 if(msn_expl_row.getCell(2) == null || msn_expl_row.getCell(3) == null) continue;
                 String msn_expl_query = msn_expl_row.getCell(2).getStringCellValue();
                 try{
                 msn_expl_query = msn_expl_row.getCell(2).getStringCellValue();
                 }catch(IllegalStateException ise){
                 continue;
                 }
                 String msn_expl_label = msn_expl_row.getCell(3).getStringCellValue();
                 System.out.println("MSN expl: " + msn_expl_query + " " + msn_expl_label);
                 msn_row[0] = msn_expl_label;
                 if(msn_expl_label.contains("Not event")){
                 msn_expl_map.put(msn_expl_query,msn_row);
                 }
                 }
                 */

                BufferedWriter aol_impl_out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_impl.csv")));
                BufferedWriter aol_expl_out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("aol_expl.csv")));
                //				BufferedWriter msn_impl_out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("msn_impl.csv")));
                //				BufferedWriter msn_expl_out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("msn_expl.csv")));

                KLDivergence kl = new KLDivergence();
                //initialize KLDivergence
                kl.init();
                //Monthly burst detection
                NYTBurstDetector monthlyBurst = new NYTBurstDetector(kl.reader, 1.0, 1, 2, 0.0);
                Set<String> aol_impl_keyset = aol_impl_map.keySet();
                Set<String> aol_expl_keyset = aol_expl_map.keySet();
                //				Set<String> msn_impl_keyset = msn_impl_map.keySet();
                //				Set<String> msn_expl_keyset = msn_expl_map.keySet();
                String label = null;

                for (String key : aol_impl_keyset) {
                    System.out.println("AOL impl: " + aol_impl_keyset.size());
                    String[] entries = aol_impl_map.get(key);
                    if (entries.length == 2 && entries[1].contains("Not event")) {
                        label = "non-event";
                    } else {
                        if (entries.length >= 7) label = entries[2] + "\t" + entries[6] + "\t" + entries[7];
                        else label = "non-event";
                    }
                    System.out.println(label);
                    int[] nyt_ts;
                    double kl_score;
                    nyt_ts = monthlyBurst.getTimeSeries_(key);
                    kl_score = kl.computeTemporalKL_PT(key);
                    aol_impl_out.write(key + "\t" + Arrays.toString(nyt_ts) + "\t" + kl_score + "\t" + label);
                    aol_impl_out.newLine();
                    aol_impl_out.flush();
                }
                aol_impl_out.close();
                for (String key : aol_expl_keyset) {
                    System.out.println("AOL expl: " + aol_expl_keyset.size());
                    String[] entries = aol_expl_map.get(key);
                    if (entries.length == 2 && entries[1].contains("Not event")) {
                        label = "non-event";
                    } else {
                        if (entries.length >= 7) label = entries[2] + "\t" + entries[6] + "\t" + entries[7];
                        else label = "non-event";
                    }
                    int[] nyt_ts;
                    double kl_score;
                    //remove year for explicit queries
                    //key = key.replaceAll("(19|20)\\d{2}", "");
                    nyt_ts = monthlyBurst.getTimeSeries_(key);
                    kl_score = kl.computeTemporalKL_PT(key);
                    aol_expl_out.write(key + "\t" + Arrays.toString(nyt_ts) + "\t" + kl_score + "\t" + label);
                    aol_expl_out.newLine();
                    aol_expl_out.flush();
                }
                aol_expl_out.close();
                /**
                 for (String key : msn_impl_keyset){
                 System.out.println("MSN impl: " + msn_impl_keyset.size());
                 String[] entries = msn_impl_map.get(key);
                 if(entries.length == 2 && entries[1].contains("Not event")){
                 label = "non-event";
                 }
                 else {
                 if(entries.length >2 && entries[4].equals("Non-recurrent") && entries[5].equals("Unplanned")) label = "unplanned_nonrecurrent";
                 else if(entries.length >2 && entries[4].equals("Non-recurrent") && entries[5].equals("Planned")) label = "planned_nonrecurrent";
                 else if(entries.length >2 && entries[4].equals("Recurrent")) label = "recurrent";
                 else label = "non-event";
                 }
                 int[] nyt_ts;
                 double kl_score;
                 nyt_ts = monthlyBurst.getTimeSeries_(key);
                 kl_score = kl.computeTemporalKL_PT(key);
                 msn_impl_out.write(key + "\t" + Arrays.toString(nyt_ts) + "\t" + kl_score + "\t" + label);
                 msn_impl_out.newLine();
                 msn_impl_out.flush();
                 }
                 msn_impl_out.close();
                 for (String key : msn_expl_keyset){
                 System.out.println("MSN expl: " + msn_expl_keyset.size());
                 String[] entries = msn_expl_map.get(key);
                 if(entries.length == 2 && entries[1].contains("Not event")){
                 label = "non-event";
                 }
                 else {
                 if(entries.length >2 && entries[4].equals("Non-recurrent") && entries[5].equals("Unplanned")) label = "unplanned_nonrecurrent";
                 else if(entries.length >2 && entries[4].equals("Non-recurrent") && entries[5].equals("Planned")) label = "planned_nonrecurrent";
                 else if(entries.length >2 && entries[4].equals("Recurrent")) label = "recurrent";
                 else label = "non-event";
                 }
                 int[] nyt_ts;
                 double kl_score;
                 //remove year for explicit queries
                 key = key.replaceAll("(19|20)\\d{2}", "");
                 nyt_ts = monthlyBurst.getTimeSeries_(key);
                 kl_score = kl.computeTemporalKL_PT(key);
                 msn_expl_out.write(key + "\t" + Arrays.toString(nyt_ts) + "\t" + kl_score + "\t" + label);
                 msn_expl_out.newLine();
                 msn_expl_out.flush();
                 }
                 msn_expl_out.close();
                 */

            } catch (InvalidFormatException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


    }

    /**
     * Click entropy of the last 5 days over last 14 days
     *
     * @param query
     * @param dataset
     * @return
     * @throws SQLException
     */
    public double[] getClickEntropyPercentage(String query, int ts_length, String dataset) throws SQLException {
        String date = getDateFromTS(dataset, ts_length);
        double CE_short = getClusterClickEntropyInPeriod(query, date, dataset, 5);
        double CE_long = getClusterClickEntropyInPeriod(query, date, dataset, 14);
        System.out.println(CE_short + " " + CE_long);
        return new double[]{CE_short, CE_long, (double) CE_short / CE_long};
    }

    /**
     * @param query
     * @param date
     * @param dataset
     * @param period
     * @return
     * @throws SQLException
     */
    public double getClusterClickEntropyInPeriod(String query, String date, String dataset, int period) throws SQLException {
        System.out.println(query + " " + date);
        LocalDate qDate = binner.parseDate(date);
        PreparedStatement stm = conn.prepareStatement("SELECT Queries FROM TemporalIR." + AOL + "qdf_partitioned_cluster WHERE (CName = ? OR Queries LIKE ?)  AND Ending_Date = ?"
                , PreparedStatement.RETURN_GENERATED_KEYS);
        stm.setString(1, query);
        stm.setString(2, "%" + query + "%");
        stm.setDate(3, new java.sql.Date(qDate.toDate().getTime()));
        stm.executeQuery();
        ResultSet rs = stm.getResultSet();
        if (!rs.next()) return 0.0d;
        String[] queries = rs.getString("Queries").split("\\;");
        int total_clicks = 0;

        HashMap<String, Integer> ufMap = new HashMap<String, Integer>();
        for (String q : queries) {
            stm = conn.prepareStatement("SELECT URL FROM TemporalIR." + dataset + "_query_url WHERE Query = ? AND Date(Time) >= ? AND Date(Time) <= ?"
                    , PreparedStatement.RETURN_GENERATED_KEYS);
            stm.setString(1, q);
            stm.setDate(2, new java.sql.Date(qDate.minusDays(period).toDate().getTime()));
            stm.setDate(3, new java.sql.Date(qDate.toDate().getTime()));
            stm.executeQuery();
            rs = stm.getResultSet();
            String url;
            int ufreq;
            while (rs.next()) {
                total_clicks++;
                url = rs.getString("URL");
                if (ufMap.containsKey(url)) {
                    ufreq = ufMap.get(url);
                    ufreq++;
                } else {
                    ufreq = 1;
                }
                ufMap.put(url, ufreq);
            }
        }
        rs.close();
        stm.close();
        Set<String> urls = ufMap.keySet(); //set of urls for the query
        double CE = 0.0d;
        double u_q;
        for (String url : urls) {
            u_q = (double) ufMap.get(url) / total_clicks;  //freq for one url over total freq for all urls
            if (u_q == 0.0) continue;
            CE += u_q * Math.log10(u_q);
        }
        return CE;
    }

    public String getDateFromTS(String dataset, int ts_length) {
        String date = null;
        if (dataset.equals(AOL)) {
            switch (ts_length) {
                case 7:
                    date = fixTimePar_aol[0];
                    break;
                case 14:
                    date = fixTimePar_aol[1];
                    break;
                case 21:
                    date = fixTimePar_aol[2];
                    break;
                case 31:
                    date = fixTimePar_aol[3];
                    break;
                case 38:
                    date = fixTimePar_aol[4];
                    break;
                case 45:
                    date = fixTimePar_aol[5];
                    break;
                case 52:
                    date = fixTimePar_aol[6];
                    break;
                case 61:
                    date = fixTimePar_aol[7];
                    break;
                case 68:
                    date = fixTimePar_aol[8];
                    break;
                case 75:
                    date = fixTimePar_aol[9];
                    break;
                case 82:
                    date = fixTimePar_aol[10];
                    break;
                case 92:
                    date = fixTimePar_aol[11];
                    break;
            }
        } else if (dataset.equals(MSN)) {
            switch (ts_length) {
                case 7:
                    date = fixTimePar_msn[0];
                    break;
                case 14:
                    date = fixTimePar_msn[1];
                    break;
                case 21:
                    date = fixTimePar_msn[2];
                    break;
                case 31:
                    date = fixTimePar_msn[3];
                    break;
            }
        }
        return date;
    }

    /**
     * extract NEs from a query
     *
     * @param query
     * @param dataset
     * @return a boolean array for [loc,per,org]
     * @throws SQLException
     */
    public int[] queryNE(String query, PreparedStatement stm) throws SQLException {
        int[] ne_array = new int[]{0, 0, 0};
        stm.setString(1, query);
        stm.executeQuery();
        ResultSet rs = stm.getResultSet();
        if (!rs.next()) return ne_array;
        String neString = rs.getString("Cap_query_entities");
        if (neString == null) return ne_array;
        neString = neString.trim();
        if (neString.equals("")) return ne_array;
        if (neString.contains("loc:")) ne_array[0] = 1;
        if (neString.contains("per:")) ne_array[1] = 1;
        if (neString.contains("org:")) ne_array[2] = 1;
        rs.close();
        return ne_array;
    }

    /**
     * @param query
     * @param ts_length
     * @param dataset
     * @return
     * @throws SQLException
     */
    public int[] queryClusterStats(String query, int ts_length, String dataset) throws SQLException {
        PreparedStatement stm = conn.prepareStatement("SELECT * FROM TemporalIR." + AOL + "qdf_partitioned_cluster WHERE CName = ? AND Ending_Date = ?",
                PreparedStatement.RETURN_GENERATED_KEYS);
        String date = getDateFromTS(dataset, ts_length);
        int[] stats = new int[4];
        stm.setString(1, query);
        stm.setString(2, date);
        stm.executeQuery();
        ResultSet rs = stm.getResultSet();
        if (!rs.next()) return stats;
        stats[0] = rs.getInt("No_Of_Queries");
        stats[1] = rs.getInt("Sum");
        stats[2] = rs.getInt("Avg");
        stats[3] = rs.getInt("MaxFreq");
        rs.close();
        stm.close();
        return stats;
    }

    public double[] getBurstDistance(SimpleBurstCollector<String, LocalDate> collector, String dataset, int ts_length) {
        LocalDate date = binner.parseDate(getDateFromTS(dataset, ts_length));
        double max_w = 0.0d;
        double cur_burst_weight = 0.0d;
        double max_l = 0.0d;
        double cur_burst_length = 0.0d;

        Burst<String, LocalDate> maxB = null;
        Burst<String, LocalDate> lastB = null;
        for (Burst<String, LocalDate> burst : collector.getBursts()) {
            cur_burst_weight = burst.getStrength();
            if (cur_burst_weight > max_w) {
                max_w = cur_burst_weight;
                maxB = burst;
            }
            cur_burst_length = burst.getDuration().toDuration().getStandardDays();
            if (cur_burst_length > max_l) max_l = cur_burst_length;
            lastB = burst;
        }
        DateMidnight date_ = date.toDateMidnight();
        int distW = Days.daysBetween(date_, maxB.getEnd().toDateMidnight()).getDays();
        int distL = Days.daysBetween(date_, lastB.getEnd().toDateMidnight()).getDays();
        return new double[]{distW, distL, max_w, max_l};

    }

    public String dynamicLabeling(DateMidnight sTime, DateMidnight eTime, DateMidnight hTime, int type) {
        System.out.println(sTime + " " + eTime + " " + hTime);
        String label;
        if (type == MEME) label = TRENDING;
        else if (Days.daysBetween(eTime, hTime).getDays() >= 20 && type == BREAKING) {
            label = TRENDING;
        } else if ((Days.daysBetween(eTime, hTime).getDays() >= 20 ||
                Days.daysBetween(hTime, sTime).getDays() >= 20)
                && (type == ONGOING)) {
            label = TRENDING;
        } else if (Days.daysBetween(hTime, sTime).getDays() <= 90 && type == ANTICIPATED) {
            label = TRENDING;
        } else {
            label = GENERAL;
        }
        return label;
    }

    public class DateFreq {
        String date;
        int freq;

        public DateFreq(String date, int freq) {
            this.date = date;
            this.freq = freq;
        }
    }

}
