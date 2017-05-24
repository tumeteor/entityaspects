package l3s.de.event.wiki.similarity;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class GeoNames {
    //cache of GeoNames
    public TIntObjectHashMap<String[]> cacheGeoNames = new TIntObjectHashMap<String[]>();

    public TIntObjectHashMap<CountryInfo> cacheCountryInfo = new TIntObjectHashMap<CountryInfo>();

    public TIntObjectHashMap<String> cacheCountry = new TIntObjectHashMap<String>();

    public TIntObjectHashMap<CityInfo> cacheCityInfo = new TIntObjectHashMap<CityInfo>();

    public static boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return false;
        }
        // only got here if we didn't return false
        return true;
    }

    public static void main(String[] args) throws SQLException {
        GeoNames geo = new GeoNames();
        geo.loadCityData("/Users/Admin/Downloads/cities1000.txt");
        TIntSet keys = geo.cacheCityInfo.keySet();
        if (keys.contains("New_York".hashCode())) {
            //TODO:
        }
    }

    public Connection connectDB3308() {
        return null;
    }

    public void closeDB(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
                System.out.println("Database connection terminated");
            } catch (Exception e) {
                /* ignore close errors */
            }
        }
    }

    public void loadGeoNamesT(Connection conn) throws SQLException {

        Statement s = conn.createStatement();
        String sql = "SELECT distinct geonameID, name, countryCode FROM " +
                "mecore.geonames";
        s.executeQuery(sql);
        ResultSet rs = s.getResultSet();
        String[] value = null;
        while (rs.next()) {
            value = new String[]{rs.getString("name"), rs.getString("countryCode")};
            cacheGeoNames.put(Integer.parseInt(rs.getString("geonameID")), value);
        }
        rs.close();
        s.close();

    }

    public void loadGeoCountryInfoT(Connection conn) throws SQLException {

        Statement s = conn.createStatement();
        String sql = "SELECT distinct cc2, country, continent, neighbours FROM " +
                "mecore.geoCountryInfo";
        s.executeQuery(sql);
        ResultSet rs = s.getResultSet();
        String[] neighbors;
        CountryInfo ci = null;
        while (rs.next()) {
            neighbors = rs.getString("neighbours").trim().split(",");
            ci = new CountryInfo(rs.getString("continent"), rs.getString("country"), rs.getString("continent"), neighbors);
            cacheCountryInfo.put(rs.getString("cc2").hashCode(), ci);

        }
        rs.close();
        s.close();
    }

    public void loadGeoCountryT(Connection conn) throws SQLException {
        Statement s = conn.createStatement();
        String sql = "SELECT distinct cc2, country FROM " +
                "mecore.geoCountryInfo";
        s.executeQuery(sql);
        ResultSet rs = s.getResultSet();

        while (rs.next()) {
            cacheCountry.put(rs.getString("country").hashCode(), rs.getString("cc2"));
        }
        rs.close();
        s.close();
    }

    public CountryInfo getCountryInfo(String country) {
        System.out.println("country: " + country);
        String cc2 = cacheCountry.get(country.hashCode());
        if (cc2 == null) return null;
        return cacheCountryInfo.get(cc2.hashCode());
    }

    public String[] getCountryNeighbors(String country) {
        String cc2 = cacheCountry.get(country.hashCode());
        CountryInfo ci = cacheCountryInfo.get(cc2.hashCode());
        return (ci == null) ? null : ci.neighbors;
    }

    public String getCountryfromCC2(String cc2) {
        CountryInfo ci = cacheCountryInfo.get(cc2.hashCode());
        return (ci == null) ? null : ci.country;
    }

    public void loadCityData(String csvPath) {
        TIntSet countryhc_set = cacheCountry.keySet();
        System.out.println("Loading csv data..");
        BufferedReader br;
        String line;
        try {
            br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(csvPath), "iso-8859-1"));
            while ((line = br.readLine()) != null) {
                String[] entry = line.split("\\t");
                //change all _ to " "
                entry[1] = entry[1].replaceAll("_", " ");
                int key = entry[1].hashCode();
                if (countryhc_set.contains(key)) continue;
                if (cacheCityInfo.containsKey(key)) {
                    if (!isInteger(entry[12])) continue;
                    if (!isInteger(cacheCityInfo.get(key).population) || Integer.parseInt(cacheCityInfo.get(key).population) < Integer.parseInt(entry[12])) {
                        cacheCityInfo.put(key, new CityInfo(entry[1], entry[8], entry[12]));
                    }

                } else {
                    cacheCityInfo.put(key, new CityInfo(entry[1], entry[8], entry[12]));
                }
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public class CountryInfo {
        public String country;
        public String continent;
        public String[] neighbors;
        public String cc2;

        public CountryInfo(String cc2, String country, String continent, String[] neighbors) {
            this.cc2 = cc2;
            this.country = country;
            this.continent = continent;
            this.neighbors = neighbors;
        }

    }

    public class CityInfo {
        public String city;
        public String countrycc2;
        public String population;

        public CityInfo(String city, String countrycc2, String population) {
            this.city = city;
            this.countrycc2 = countrycc2;
            this.population = population;
        }
    }

}
