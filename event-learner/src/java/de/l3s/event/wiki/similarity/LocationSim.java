package l3s.de.event.wiki.similarity;

import au.com.bytecode.opencsv.CSVReader;
import com.hp.hpl.jena.query.*;
import de.l3s.common.WeightedJaccard;
import gnu.trove.set.TIntSet;
import l3s.de.event.wiki.similarity.GeoNames.CountryInfo;
import org.joda.time.LocalDate;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;


public class LocationSim {
    public static final int CITY = 0;
    public static final int REGION = 1;
    public static final int COUNTRY = 2;
    public static final int CONTINENT = 3;
    public GeoNames geo = new GeoNames();
    public Connection conn;
    NLPUtils neextractor = new NLPUtils();
    LocalDateBinner binner = new LocalDateBinner();

    public LocationSim() {
        conn = geo.connectDB3308();
        try {
            geo.loadGeoCountryInfoT(conn);
            geo.loadGeoCountryT(conn);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public LocationSim(String csvPath) {
        conn = geo.connectDB3308();
        try {
            geo.loadGeoCountryInfoT(conn);
            geo.loadGeoCountryT(conn);
            geo.loadCityData(csvPath);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static Document readXMLFromUrl(String url) throws ParserConfigurationException, MalformedURLException, SAXException, IOException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new URL(url).openStream());
        return doc;

    }

    public static void main(String[] args) {
        LocationSim ls = new LocationSim("/Users/Admin/Downloads/cities1000.txt");
        System.out.println(ls.calLocationSim("New_York, Quebec, Vermont", "Mississippi_embayment, United_States"));
//		LocationSim ls = new LocationSim();
//		HashMap<String, EventInfo> eventmap = ls.loadcsv("/event_list/atlantic_hurricane_seasons.csv");
//		Set<String> keyset = eventmap.keySet();
//		ArrayList<String> trg_events = new ArrayList<String>();
//		LocalDate starting_point = ls.binner.parseDate("2008-01-01");
//		for(String expr : keyset){
//			if(eventmap.get(expr).startingDate.isAfter(starting_point) && !trg_events.contains(expr))
//				trg_events.add(expr);
//		}
//		double sim_score = 0.0d;
//		EventInfo eventinfo = null;
//		EventInfo s_eventinfo = null;
//		for (String event : trg_events){
//			eventinfo = eventmap.get(event);
//			for (String s_event : keyset) {
//				s_eventinfo = eventmap.get(s_event);
//				if (s_eventinfo.endingDate.compareTo(eventinfo.startingDate) > 0) continue;
//				sim_score = ls.calLocationSim(eventinfo.location, s_eventinfo.location);
//
//				System.out.println(event + " - " + s_event + " : " + sim_score);
//
//			}
//		}
//


    }

    public Info calLocationSim(String t1, String t2) {
        List<String> locs1 = getLocationHierarchicalList3(t1);
        List<String> locs2 = getLocationHierarchicalList3(t2);
        double score = WeightedJaccard.jaccardSimilarity(locs1, locs2);

        return new Info(locs1, locs2, score);
    }

    public List<String> getLocationHierarchicalList3(String text) {

        TIntSet cityhc_set = geo.cacheCityInfo.keySet();
        TIntSet countryhc_set = geo.cacheCountry.keySet();
        //set of locations: city - region - country - continent
        List<String> DBPediaLocs = new ArrayList<String>();
        //exclude zero-length text
        if (text.length() == 0) return DBPediaLocs;
        List<String> locs = extractLocation(text);
        for (String loc : locs) {
            System.out.println("tag: " + loc);
            int lochc = loc.hashCode();
            System.out.println(loc);
            if (cityhc_set.contains(lochc)) {
                System.out.println("city: " + loc);
                //is a city
                // x4
                DBPediaLocs.add(loc);
                DBPediaLocs.add(loc);
                DBPediaLocs.add(loc);
                DBPediaLocs.add(loc);
                //get cc2  from csv file is more reliable e.g., Berlin
                String cc2 = geo.cacheCityInfo.get(lochc).countrycc2;
                System.out.println("cc2: " + cc2);
                String country = geo.getCountryfromCC2(cc2);
                if (country == null) continue;
                System.out.println("country: " + country);
                // x2
                DBPediaLocs.add(loc);
                DBPediaLocs.add(loc);

                CountryInfo ci = geo.getCountryInfo(country);
                if (ci == null) continue;
                if (ci.neighbors.length == 0) continue;
                for (String neighborcc2 : ci.neighbors) {
                    if (neighborcc2 == null || neighborcc2.trim().equals("")) continue;
                    System.out.println("neighbor: " + neighborcc2);
                    // neighbor x1
                    DBPediaLocs.add(geo.getCountryfromCC2(neighborcc2));
                    System.out.println("neighbor: " + geo.getCountryfromCC2(neighborcc2));
                }

                //continent x1
                System.out.println(ci.continent);
                DBPediaLocs.add(ci.continent);

            } else if (countryhc_set.contains(loc.hashCode())) {
                System.out.println("country: " + loc);
                DBPediaLocs.add(loc);
                String country = loc;
                CountryInfo ci = geo.getCountryInfo(country);
                if (ci == null) continue;
                if (ci.neighbors.length == 0) continue;
                for (String neighborcc2 : ci.neighbors) {
                    if (neighborcc2 == null || neighborcc2.trim().equals("")) continue;
                    System.out.println("neighbor: " + neighborcc2);
                    // neighbor x1
                    DBPediaLocs.add(geo.getCountryfromCC2(neighborcc2));
                    System.out.println("neighbor: " + geo.getCountryfromCC2(neighborcc2));
                }

                //continent x1
                System.out.println(ci.continent);
                DBPediaLocs.add(ci.continent);
            }

        }

        return DBPediaLocs;

    }

    public Set<String> getLocationHierarchicalList2(String text) {
        //set of locations: city - region - country - continent
        Set<String> DBPediaLocs = new HashSet<String>();
        //exclude zero-length text
        if (text.length() == 0) return DBPediaLocs;
        List<String> locs = extractLocation(text);
        for (String loc : locs) {
            //get resource URI from DBPedia
            String uri = getDBPediaResourceURL(loc);
            if (uri == null) {
                DBPediaLocs.add(loc);
                continue;
            }
            String sparql_cou = getCountrySparql(uri);
            DBPediaLocs.add(uri);
            int type = getDBPediaType(uri);
            if (type == CITY || type == REGION) {
                System.out.println("GEO: " + uri);
                if (type == CITY) {
                    // city x4 weight
                    DBPediaLocs.add(uri);
                    DBPediaLocs.add(uri);
                }
                // region x3 weight
                DBPediaLocs.add(uri);
                List<String> c;
                String country = null;
                if (!(c = queryDBPedia(sparql_cou)).isEmpty()) {
                    country = c.get(0);
                } else if (!(c = queryDBPedia(getCountrySparql_(uri))).isEmpty()) {
                    country = c.get(0);
                }
                if (country != null) {
                    // country x2 weight
                    DBPediaLocs.add(country);
                    DBPediaLocs.add(country);

                    //remove DBPedia namespace
                    country = country.replace("http://dbpedia.org/resource/", "");
                    CountryInfo ci = geo.getCountryInfo(country);
                    if (ci == null) continue;
                    if (ci.neighbors.length == 0) continue;
                    for (String neighborcc2 : ci.neighbors) {
                        if (neighborcc2 == null || neighborcc2.trim().equals("")) continue;
                        System.out.println("neighbor: " + neighborcc2);
                        // neighbor x1
                        DBPediaLocs.add(geo.getCountryfromCC2(neighborcc2));
                        System.out.println("neighbor: " + geo.getCountryfromCC2(neighborcc2));
                    }

                    //continent x1
                    DBPediaLocs.add(ci.continent);
                }

            }


        }

        return DBPediaLocs;

    }

    /**
     * TODO: customize the lower level weight
     *
     * @param text
     * @return
     */
    public Set<String> getLocationHierarchicalList(String text) {
        //set of locations: city - region - country - continent
        Set<String> DBPediaLocs = new HashSet<String>();
        //exclude zero-length text
        if (text.length() == 0) return DBPediaLocs;
        List<String> locs = extractLocation(text);
        for (String loc : locs) {
            //get resource URI from DBPedia
            String uri = getDBPediaResourceURL(loc);
            if (uri == null) {
                DBPediaLocs.add(loc);
                continue;
            }
            String sparql_cou = getCountrySparql(uri);
            String sparql_con = getContinentSparql(uri);
            String resource = getDBPediaResourceURL(loc);
            DBPediaLocs.add(resource);
            int type = getDBPediaType(resource);
            if (type == CITY || type == REGION) {
                if (type == CITY) {
                    DBPediaLocs.add(resource);
                }
                DBPediaLocs.add(resource);
                List<String> c;
                String country = null;
                if (!(c = queryDBPedia(sparql_cou)).isEmpty()) {
                    country = c.get(0);
                } else if (!(c = queryDBPedia(getCountrySparql_(uri))).isEmpty()) {
                    country = c.get(0);
                }
                if (country != null) DBPediaLocs.add(country);
                List<String> cons = queryDBPedia(getContinentSparql(country));
                if (!cons.isEmpty()) {
                    String continent = cons.get(0);
                    DBPediaLocs.add(continent);
                }
            } else if (getDBPediaType(resource) == COUNTRY) {
                String continent = (!queryDBPedia(sparql_con).isEmpty()) ? queryDBPedia(sparql_con).get(0) : null;
                if (continent != null) DBPediaLocs.add(continent);
            }

        }

        return DBPediaLocs;

    }

    public String getCitySparql(String uri) {
        return "select distinct ?o where {<" + uri + "> <http://dbpedia.org/ontology/city> ?o} LIMIT 1";
    }

    public String getCitySparql_(String uri) {
        return "select distinct ?o where {<" + uri + "> <http://umbel.org/umbel/rc/City> ?o} LIMIT 1";
    }

    public String getCountrySparql(String uri) {
        return "select distinct ?o where {<" + uri + "> <http://dbpedia.org/ontology/country> ?o} LIMIT 1";
    }

    public String getCountrySparql_(String uri) {
        return "select distinct ?o where {<" + uri + "> <http://umbel.org/umbel/rc/Country> ?o} LIMIT 1";
    }

    public String getContinentSparql(String uri) {
        return "select distinct ?o where {<" + uri + "> <http://dbpedia.org/ontology/continent> ?o} LIMIT 1";
    }

    public String getContinentSparql_(String uri) {
        return "select distinct ?o where {<" + uri + "> <http://umbel.org/umbel/rc/Continent> ?o} LIMIT 1";
    }

    /**
     * @param text
     * @return
     */
    public List<String> extractLocation(String text) {
        ArrayList<String> locs = new ArrayList<String>();
        if (text.length() == 0) return locs;
        //removed wrong unicode text
        text = neextractor.ignoreNonCharWords(text);
//		if (text.length() > 120) text = text.substring(0, 120);
        String annotated = neextractor.extractEntitiesfromSentence(text);
        String[] tags = annotated.split(",");
        for (String tag : tags) {
            if (tag.contains("loc:")) {
                if (tag.startsWith(" ")) tag = tag.replaceFirst(" ", "");
                tag = tag.replace("loc:", "");
                //special matching cases
                if (tag.equals("Burma")) locs.add("Myanmar"); //
                if (tag.equals("KwaZulu-Natal")) locs.add("South Africa");
                if (tag.contains("Congo") && !tag.contains("Democratic")) locs.add("Republic of the Congo");
                if (tag.contains("Congo") && !tag.contains("Democratic")) locs.add("Democratic Republic of the Congo");
                if (tag.contains("Antigua") || tag.contains("Barbuda")) locs.add("Antigua and Barbuda");
                if (tag.contains("DR Congo")) locs.add("Democratic Republic of the Congo");
                if (!tag.equals("")) locs.add(tag);
            }
        }
        return locs;
    }

    public String getDBPediaResourceURL(String loc) {
        String r_url = null;
        try {
            Document doc = readXMLFromUrl("http://lookup.dbpedia.org/api/search.asmx/KeywordSearch?QueryClass=place&QueryString=" + loc);
            NodeList nodeList = doc.getElementsByTagName("URI");
            if (nodeList.getLength() == 0) return null;
            //get the 1st result
            r_url = nodeList.item(0).getTextContent();
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SAXException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return r_url;

    }

    /**
     * @param sparql
     * @return
     */
    public List<String> queryDBPedia(String sparql) {

        List<String> ls = new ArrayList<String>();
        Query query = QueryFactory.create(sparql);
        QueryExecution qexec = QueryExecutionFactory.sparqlService("http://dbpedia.org/sparql", query);

        ResultSet results = qexec.execSelect();
        if (!results.hasNext()) return ls;
        while (results.hasNext()) {
            QuerySolution qs = results.next();
            ls.add(qs.get("?o").toString());
        }

        qexec.close();
        return ls;
    }

    public int getDBPediaType(String uri) {
        String sparql = "select distinct ?o where {<" + uri + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o} LIMIT 100";
        List<String> types = queryDBPedia(sparql);
        if (types.size() == 0) return -1;
        if (types.contains("http://dbpedia.org/ontology/city") || types.contains("http://umbel.org/umbel/rc/City")
                || types.contains("http://umbel.org/umbel/rc/Village")) return CITY;
        else if (types.contains("http://dbpedia.org/ontology/country")) return COUNTRY;
        else if (types.contains("http://dbpedia.org/ontology/continent")) return CONTINENT;
        else if (types.contains("http://dbpedia.org/ontology/region")) return CONTINENT;
        else return -1;

    }

    public HashMap<String, EventInfo> loadcsv(String path) {
        HashMap<String, EventInfo> eventmap = new HashMap<String, EventInfo>();
        CSVReader reader;
        try {
            reader = new CSVReader(new InputStreamReader(getClass().getResourceAsStream(path)), ',');
            String[] entry;
            //exclude titles
            reader.readNext();
            //event name - location
            while ((entry = reader.readNext()) != null) {
                eventmap.put(entry[0], new EventInfo(binner.parseDate(entry[2]), binner.parseDate(entry[3]), entry[4]));
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return eventmap;
    }

    public class Info {
        public List<String> locs1;
        public List<String> locs2;
        public double score;

        public Info(List<String> locs1, List<String> locs2, double score) {
            this.locs1 = locs1;
            this.locs2 = locs2;
            this.score = score;
        }
    }

    class EventInfo {
        public LocalDate startingDate;
        public LocalDate endingDate;
        public String location;

        public EventInfo(LocalDate startingDate, LocalDate endingDate, String location) {
            this.startingDate = startingDate;
            this.endingDate = endingDate;
            this.location = location;
        }
    }

}
