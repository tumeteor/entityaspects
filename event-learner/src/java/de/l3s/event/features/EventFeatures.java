package l3s.de.event.features;

import au.com.bytecode.opencsv.CSVReader;
import de.l3s.timesynonym.burst.NYTBurstDetector;
import de.l3s.tools.Burst;
import de.l3s.tools.QueryLogBurstDetector;
import de.l3s.tools.SimpleBurstCollector;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;
import org.joda.time.LocalDate;
import scala.actors.threadpool.Arrays;

import java.io.*;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class EventFeatures {
    public Connection conn;
    String[] fixTimePar_msn = {"2006-05-07", "2006-05-14", "2006-05-21", "2006-05-31"};
    String[] fixTimePar_aol = {"2006-03-07", "2006-03-14", "2006-03-21", "2006-03-31",
            "2006-04-07", "2006-04-14", "2006-04-21", "2006-04-30",
            "2006-05-07", "2006-05-14", "2006-05-21", "2006-05-31"};

    public EventFeatures() {
        conn = connectDB3306();
    }

    /**
     * @return
     * @throws URISyntaxException
     */
    public static ArrayList<String> tempExList() throws URISyntaxException {
        ArrayList<String> tempExList = new ArrayList<String>();
        File resultFolder = new File("src/main/resources/ALL explicit queries/");
        if (resultFolder.exists() && resultFolder.isDirectory()) {
            File[] files = resultFolder.listFiles();
            for (int i = 0; i < files.length; i++) {
                File tempExF = files[i];
                if (tempExF.length() == 0) {
                    continue;
                }
                try {
                    String[] entry;
                    String query;
                    CSVReader reader = new CSVReader(new FileReader(tempExF), '\t');
                    while ((entry = reader.readNext()) != null) {
                        query = entry[2].replaceAll("(19|20)\\d{2}", "");
                        tempExList.add(query);
                    }
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }
        }
        return tempExList;

    }

    public static void main(String args[]) {
//		EventFeatures eventFeatures = new EventFeatures();
//		eventFeatures.computeFeatures_MainClass();
        try {
            for (String a : EventFeatures.tempExList())
                System.out.println(a);
        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void computeFeatures_MainClass() {
        try {
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("queries_labeled_additionalfeatures(sse+bursts+isTemp).arff")));
            // Write header
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
            //out.write("@attribute class_LMTU {time, nontime}");
            //out.write("@attribute class {recurrent ,planned_nonrecurrent, unplanned_nonrecurrent, non-event}");
            out.write("@attribute class {event, non-event}");
            out.newLine();
            out.write("@data");
            out.newLine();
            RFeatures rFeatures = new RFeatures();
            int index = 0;
            InputStream inp = new FileInputStream("workbook.xlsx");
            Workbook wb;
            try {
                wb = WorkbookFactory.create(inp);
                Sheet sheet = wb.getSheetAt(0);
                Row row = sheet.getRow(2);
                Cell cell = row.getCell(3);
            } catch (InvalidFormatException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            CSVReader labeled_reader = new CSVReader(new InputStreamReader(getClass().getResourceAsStream("/event-related_queries_labeled_manual_final_120113.csv")), ',');
            CSVReader nyt_reader = new CSVReader(new InputStreamReader(getClass().getResourceAsStream("/nytFeatures.csv")), '\t');
            CSVReader querylog_reader = new CSVReader(new InputStreamReader(getClass().getResourceAsStream("/simulateQtimeSeries.csv")), '\t');
            String[] entry;
            //store nyt timeseries csv file in hashmap
            HashMap<String, TS_KLPair> nyt_map = new HashMap<String, TS_KLPair>();
            while ((entry = nyt_reader.readNext()) != null) {
                String[] nyt_ts_array = entry[1].substring(1, entry[1].length() - 1).split("\\,");
                int[] nyt_ts = new int[nyt_ts_array.length];
                for (int i = 0; i < nyt_ts_array.length; i++) {
                    nyt_ts[i] = Integer.parseInt(nyt_ts_array[i].trim());
                }
                nyt_map.put(entry[0], new TS_KLPair(nyt_ts, Double.parseDouble(entry[2])));
            }
            //store querylog timeseries csv file in hashmap
            HashMap<String, ArrayList<int[]>> querylog_map = new HashMap<String, ArrayList<int[]>>();
            while ((entry = querylog_reader.readNext()) != null) {
                String[] ql_ts_array = entry[2].substring(1, entry[2].length() - 1).split("\\,");
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
                tempExList = tempExList();
            } catch (URISyntaxException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            while ((entry = labeled_reader.readNext()) != null) {
                String label = "";
                String query = entry[0];
                if (entry[3].startsWith("0:")) label = "non-event";
                else {
                    if (entry[4].equals("Non-recurrent") && entry[5].equals("Unplanned")) label = "event";
                    else if (entry[4].equals("Non-recurrent") && entry[5].equals("Planned")) label = "event";
                    else if (entry[4].equals("Recurrent")) label = "event";
                    else label = "non-event";
                }
                double kl_score = nyt_map.get(query).kl_score;
                //clean up explicit queries
                //for monthly granularity, freq = 12
                int[] nyt_ts = nyt_map.get(query).ts;

                //autocorrelation
                double[] acf = rFeatures.computeAutoCorrel(12, nyt_ts);
                //seasonality value for NYT collection
                double long_span_seasonal = rFeatures.decomposeTSSeasonalityfromNYTwithCosineSim(nyt_ts);
                //kurtosis
                double kurtosis_ts = rFeatures.getKurtosisTimeSeries(nyt_ts);

                if (!querylog_map.containsKey(query)) {
                    int sum_ts = 0;
                    for (int i : nyt_ts) {
                        sum_ts += i;
                    }
                    if (sum_ts == 0) {
                        out.write(index + ",?,?,?,?,?,?,?,?,?,?,?,?,?,?," + label);
                    } else {
                        out.write(index + "," + acf[0] + "," + long_span_seasonal + "," + kl_score + "," + kurtosis_ts + ",?,?,?,?,?,?,?,?,?,?," + label);
                    }
                    out.newLine();
                    out.flush();
                    index++;
                } else {
                    ArrayList<int[]> tsList = querylog_map.get(query);
                    for (int[] querylog_ts : tsList) {
                        //autocorrelation
                        double[] short_acf = rFeatures.computeAutoCorrel(12, querylog_ts);
                        //seasonality value for QueryLog collection
                        double short_span_seasonal;
                        //Holt-Winters prediction SSE
                        double prediction_sse;
                        //for querylog timeseries, set Freq = 3
                        //max burst weight
                        double burst_weight = 0.0;
                        //max burst_length;
                        double burst_length = 0.0;
                        //number of bursts
                        int no_of_bursts = 0;
                        int sum_ts_ql = 0;
                        for (int i : querylog_ts) {
                            sum_ts_ql += i;
                        }
                        if (querylog_ts.length < 6 || sum_ts_ql == 0) {
                            short_span_seasonal = Double.NaN;
                            prediction_sse = Double.NaN;
                        } else {
                            short_span_seasonal = rFeatures.decomposeTSSeasonalityfromQLwithCosineSim(querylog_ts);
                            prediction_sse = rFeatures.exponentialSmoothingQLTimeseries(querylog_ts)[0];
                            QueryLogBurstDetector burstDetector;
                            SimpleBurstCollector<String, LocalDate> collector = null;
                            if (entry[1].startsWith("MSN")) {
                                if (querylog_ts.length <= QueryLogBurstDetector.MSNQueryFreq.length) {
                                    burstDetector = new QueryLogBurstDetector(conn, "2006-05-01", querylog_ts.length, "msn");
                                    collector = burstDetector.detectTermBursts(query, querylog_ts);
                                }
                            } else {
                                if (querylog_ts.length <= QueryLogBurstDetector.AOLQueryFreq.length) {
                                    burstDetector = new QueryLogBurstDetector(conn, "2006-03-01", querylog_ts.length, "aol");
                                    collector = burstDetector.detectTermBursts(query, querylog_ts);
                                }
                            }
                            if (collector == null || collector.getBursts().size() == 0) {
                                no_of_bursts = 0;
                                burst_weight = Double.NaN;
                                burst_length = Double.NaN;
                            } else {
                                no_of_bursts = collector.getBursts().size();
                                for (Burst<String, LocalDate> burst : collector.getBursts()) {
                                    long cur_burst_length = burst.getDuration().toDuration().getStandardDays();
                                    if (cur_burst_length > burst_length) burst_length = cur_burst_length;
                                    double cur_burst_weight = burst.getStrength();
                                    if (cur_burst_weight > burst_weight) burst_weight = cur_burst_weight;
                                }
                            }

                        }
                        //isTempEx
                        int isTempEx = (tempExList.contains(query)) ? 1 : 0;
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
                        if (sum_ts == 0) {
                            out.write(index + ",?,?,?,?," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + label);
                        } else {
                            out.write(index + "," + acf[1] + "," + long_span_seasonal + "," + kl_score + "," + kurtosis_ts + "," + short_acf[1] + "," + short_span_seasonal + "," + short_kurtosis_ts + "," + prediction_sse + "," + max + "," + (double) sum / querylog_ts.length + "," + burst_weight + "," + burst_length + "," + no_of_bursts + "," + isTempEx + "," + label);
                        }
                        out.newLine();
                        out.flush();
                        index++;

                    }
                }

                /**
                 HashMap<String,QueryStats> queryStatsMap = null;
                 if(dataset.equals("msn")) queryStatsMap = queryMSNStatsMap;
                 else queryStatsMap = queryAOLStatsMap;
                 QueryStats queryStats = queryStatsMap.get(query);
                 String querylog_ts =  queryStats.timeSeries.substring(1, queryStats.timeSeries.length()-1);
                 String[] ql_ts = querylog_ts.split("\\,");
                 int[] ql_ts_ = new int[ql_ts.length];
                 for(int i = 0; i < ql_ts.length; i++){
                 ql_ts_[i] = Integer.parseInt(ql_ts[i]);
                 }
                 String burst_period = queryStats.burst_Period.substring(1, queryStats.burst_Period.length()-1);
                 String[] burst_elements = burst_period.split("\\,");
                 double burst_weight = Double.parseDouble(burst_elements[2]);
                 DateMidnight start_burst = new DateMidnight(burst_elements[0]);
                 DateMidnight end_burst = new DateMidnight(burst_elements[1]);
                 int burst_length = Days.daysBetween(start_burst, end_burst).getDays();
                 */
            }
            out.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Function for computing non-R Features
     */
    public void computeNonRFeatures() {
        try {
            InputStream inp = new FileInputStream("workbook.xlsx");
            Workbook wb = WorkbookFactory.create(inp);
            Sheet sheet = wb.getSheetAt(0);
            Row row = sheet.getRow(2);
            Cell cell = row.getCell(3);
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("nonRFeatures.csv")));
            KLDivergence kl = new KLDivergence();
            //initialize KLDivergence
            kl.init();
            //Monthly burst detection
            NYTBurstDetector monthlyBurst = new NYTBurstDetector(kl.reader, 1.0, 1, 2, 0.0);
            CSVReader reader = new CSVReader(new FileReader("event-related_queries_labeled_manual_final_120113.csv"), ',');
            String[] entry;
            while ((entry = reader.readNext()) != null) {
                String label = "";
                String query = entry[0];
                if (entry[3].startsWith("0:")) label = "non-event";
                else {
                    if (entry[4].equals("Non-recurrent") && entry[5].equals("Unplanned"))
                        label = "unplanned_nonrecurrent";
                    if (entry[4].equals("Non-recurrent") && entry[5].equals("Planned")) label = "planned_nonrecurrent";
                    if (entry[4].equals("Recurrent")) label = "recurrent";
                }

                //clean up explicit queries
                int[] nyt_ts;
                double kl_score;
                if (entry[1].endsWith("expl")) {
                    String expl_query = query.replaceAll("(19|20)\\d{2}", "");
                    System.out.println(expl_query);
                    nyt_ts = monthlyBurst.getTimeSeries_(expl_query);
                    kl_score = kl.computeTemporalKL_PT(expl_query);
                } else {
                    nyt_ts = monthlyBurst.getTimeSeries_(query);
                    kl_score = kl.computeTemporalKL_PT(query);
                }
                out.write(query + "\t" + Arrays.toString(nyt_ts) + "\t" + kl_score + "\t" + label);
                out.newLine();
                out.flush();
            }
            out.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvalidFormatException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
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
            System.err.println("Cannot connect to database server");
        }

        return conn;
    }

    public class TS_KLPair {
        public int[] ts;
        public double kl_score;

        public TS_KLPair(int[] ts, double kl_score) {
            this.ts = ts;
            this.kl_score = kl_score;
        }
    }

}
