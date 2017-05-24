package l3s.de.event.features;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class QueryLogFeatures {
    public Connection conn;

    public QueryLogFeatures() {
        conn = connectDB3306();
    }

    /**
     * @return
     */
    public Connection connectDB3306() {

        return null;
    }

    /**
     * @param dataset
     * @param query
     * @return
     */
    public QueryStats getQueryStats(String dataset, String query) {
        String q = "SELECT Query, Burst_Period, TimeSeries, Sum, MaxFreq, No_Of_Queries FROM TemporalIR." + dataset + "_merged_burst, "
                + "TemporalIR." + dataset + "qdf_merged_cluster WHERE TemporalIR." + dataset + "qdf_merged_cluster.CID = TemporalIR." + dataset + "_merged_burst.CID AND ( Query = ? OR Queries LIKE ? ) ";
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(q, PreparedStatement.RETURN_GENERATED_KEYS);
            stmt.setString(1, query);
            stmt.setString(2, "%" + query + "%");
            ResultSet rs = stmt.executeQuery();
            rs.next();
            QueryStats queryStats = new QueryStats(query, rs.getString("TimeSeries"), rs.getString("Burst_Period"), rs.getInt("Sum"), rs.getInt("MaxFreq"), rs.getInt("No_Of_Queries"));
            stmt.close();
            return queryStats;
        } catch (SQLException e) {
            try {
                stmt.close();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            System.out.println("Error at query: " + query);
        }

        return null;

    }

    /**
     * @param dataset
     * @return
     */
    public HashMap<String, QueryStats> getQueryStats(String dataset) {
        String q = "SELECT Query, Burst_Period, TimeSeries, Sum, MaxFreq, No_Of_Queries FROM TemporalIR." + dataset + "_merged_burst_copy, "
                + "TemporalIR." + dataset + "qdf_merged_cluster WHERE TemporalIR." + dataset + "qdf_merged_cluster.CID = TemporalIR." + dataset + "_merged_burst_copy.CID ";
        Statement s = null;
        try {
            s = conn.createStatement();
            ResultSet rs = s.executeQuery(q);
            HashMap<String, QueryStats> queryStatsMap = new HashMap<String, QueryStats>();
            while (rs.next()) {
                String query = rs.getString("Query");
                QueryStats queryStats = new QueryStats(query, rs.getString("TimeSeries"), rs.getString("Burst_Period"), rs.getInt("Sum"), rs.getInt("MaxFreq"), rs.getInt("No_Of_Queries"));
                queryStatsMap.put(query, queryStats);
            }

            s.close();
            return queryStatsMap;
        } catch (SQLException e) {
            try {
                s.close();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }

        return null;

    }

    /**
     * @param dataset
     * @param query
     * @return
     */
    public QueryStats getQueryStats(String dataset, HashMap<String, QueryStats> queryStatsMap, String query) {
        queryStatsMap = getQueryStats(dataset);
        return queryStatsMap.get(query);
    }

    public ArrayList<String> getClickedURLs(String query, String sDate, String eDate) {
        ArrayList<String> urls = new ArrayList<String>();
        String q = "SELECT URL FROM TemporalIR.aol_query_url WHERE Query = \"" + query + "\" AND Time >= '" + sDate + "%' AND Time <= '" + eDate + "%' ";
        Statement s = null;
        try {
            s = conn.createStatement();
            ResultSet rs = s.executeQuery(q);
            while (rs.next()) {
                urls.add(rs.getString("URL"));
            }

            s.close();
        } catch (SQLException e) {
            try {
                s.close();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
        return urls;

    }

    public ArrayList<String> getClickedURLs(String query) {
        ArrayList<String> urls = new ArrayList<String>();
        String q = "SELECT URL FROM TemporalIR.aol_query_url WHERE Query = \"" + query + "\"";
        Statement s = null;
        try {
            s = conn.createStatement();
            ResultSet rs = s.executeQuery(q);
            while (rs.next()) {
                urls.add(rs.getString("URL"));
            }

            s.close();
        } catch (SQLException e) {
            try {
                s.close();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
        return urls;

    }

    public HashMap<String, ArrayList<String>> getClickedURLsBySession(String query) {
        HashMap<String, ArrayList<String>> su_map = new HashMap<String, ArrayList<String>>();
        String q = "SELECT SessionID, URL FROM TemporalIR.aol_query_url WHERE Query = \"" + query + "\" GROUP BY SessionID";
        Statement s = null;
        try {
            s = conn.createStatement();
            ResultSet rs = s.executeQuery(q);
            while (rs.next()) {
                String url = rs.getString("URL");
                String sessionId = rs.getString("SessionID");
                ArrayList<String> urls = null;
                if (su_map.containsKey(sessionId)) {
                    urls = su_map.get(sessionId);
                } else {
                    urls = new ArrayList<String>();
                }
                urls.add(url);
                su_map.put(sessionId, urls);
            }
            s.close();
        } catch (SQLException e) {
            try {
                s.close();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
        return su_map;

    }

    public class QueryStats {
        public String query;
        public String timeSeries;
        public String burst_Period;
        public int sum;
        public int no_Of_Queries;
        public int maxFreq;

        public QueryStats(String query, String timeSeries, String burst_Period, int sum, int no_Of_Queries, int maxFreq) {
            this.query = query;
            this.timeSeries = timeSeries;
            this.burst_Period = burst_Period;
            this.sum = sum;
            this.no_Of_Queries = no_Of_Queries;
            this.maxFreq = maxFreq;
        }
    }

}
