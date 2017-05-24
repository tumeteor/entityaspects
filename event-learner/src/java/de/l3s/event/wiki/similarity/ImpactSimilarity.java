package l3s.de.event.wiki.similarity;


import de.l3s.tools.LocalDateBinner;
import org.apache.commons.cli.*;
import org.joda.time.LocalDate;
import org.joda.time.Months;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ImpactSimilarity {
    public static LocalDateBinner binner = new LocalDateBinner();
    //starting date for the triggering event
    public static LocalDate startingTriggerDate = binner.parseDate("2008-01-01");
    public String startingDate = "Starting Date";
    public String endingDate = "Ending Date";
    public String fatalities = "Fatalities";
    public String magnitude = "Magnitude";
    public String damage = "Damage";

    /**
     * @param args
     * @throws ParseException
     */
    public static void main(String[] args) throws ParseException {

        ImpactSimilarity is = new ImpactSimilarity();

        // create the command line parser
        CommandLineParser parser = new GnuParser();

        // create Options object
        Options options = new Options();

        options.addOption("c", "categories", true, "is a directory path to event categories and metadata");


        // parse the command line arguments
        CommandLine line = parser.parse(options, args);

        //event category
        String categoryFolder = line.hasOption("c") ? line.getOptionValue("c") : "/Users/Admin/Work/entity research/event-learner/src/main/resources/event_categories";

        //		String outputDir = "/home/kanhabua/projects/deduplication/data/WikiEvents/impact_results_151213";

        // Iterate over all categories
        File inputFile = new File(categoryFolder);
        if (inputFile.exists() && inputFile.isDirectory()) {

            File[] files = inputFile.listFiles();
            Arrays.sort(files);
            int noOfFiles = files.length;
            for (int f = 0; f < noOfFiles; f++) {
                String c_file = files[f].getName();
                System.out.println(c_file);
                String c_path = files[f].getAbsolutePath();
                String category = c_file.substring(0, c_file.indexOf("."));
                String type = is.getEventType(category);

                // Ignore planned events where impact factors will not be determined
                if (type.equals("p")) {
                    continue;
                }

                System.out.println("===================== Category[" + category + ", type=" + type + "] =====================");

                //load event metatdata from csv file
                //HashMap<String, EventPeriod> event_map = r.getEventMetadata_(c_path);
                HashMap<String, EventPeriod> event_map = new HashMap<String, EventPeriod>();
                HashMap<String, String> damage_map = new HashMap<String, String>();
                HashMap<String, String> fatality_map = new HashMap<String, String>();
                HashMap<String, String> magnitude_map = new HashMap<String, String>();

                TreeMap<String, EventPeriod> sorted_event_map = is.getEventMetadata(c_path, event_map, damage_map, fatality_map, magnitude_map);

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

                //int eid = 0;
                int allEvents = all_events_list.size();
                LocalDate st_event;
                LocalDate et_event;

                //String regex = "^\\d+[.,]\\d+$";
                String number_regex = "([1-9](?:\\d{0,5})(?:,\\d{3})*(?:\\.\\d*[1-9])?|0?\\.\\d*[1-9]|0)";
                Pattern p_damage1_1 = Pattern.compile("([1-9](?:\\d{0,2})(?:,\\d{3})*(?:\\.\\d*[1-9])?|0?\\.\\d*[1-9]|0)?\\s+(acres|acre)");    // 1,500 acres or 1.1 million acres
                Pattern p_damage1_2 = Pattern.compile("\\d+[.,]\\d+\\s+?(million acres)");    // 1,500 acres or 1.1 million acres
                Pattern p_damage2 = Pattern.compile("\\d+\\s+?(buildings|structures|residents)");
                Pattern p_damage3 = Pattern.compile("[$]\\d+[.,]\\d+");
                Pattern p_damage3_1 = Pattern.compile("[$]\\d+[.,]*\\d+\\s?(million|billion)|\\d+[.,]\\d+\\s+?(million|billion)\\s+?(USD|usd)");
                Pattern p_magnitude1 = Pattern.compile(number_regex);    // 7.5
                Pattern p_magnitude2 = Pattern.compile("\\b1MinWinds " + number_regex); // 1MinWinds 80, pressure 976
                Pattern p_magnitude3 = Pattern.compile("\\bpressure " + number_regex); // 1MinWinds 80, pressure 976
                Pattern p_fatality = Pattern.compile(number_regex);

                //for (String main_event : trg_events) {
                System.out.println("==== Start extracting temporal features ====");
                for (int eid = 0; eid < allEvents; eid++) {
                    String event = all_events_list.get(eid);
                    if (event_map.get(event) == null) {
                        System.out.println("Error: event_map.get(" + event + ") is null");
                        continue;
                    }

                    System.out.println("Current event: [" + eid + "] " + event);

                    st_event = event_map.get(event).startingDate;
                    et_event = event_map.get(event).endingDate;

                    boolean isTriggeringEvent = st_event.isAfter(startingTriggerDate);

                    if (!isTriggeringEvent) {
                        System.out.println("\tSkip this event.");
                        continue;
                    }

                    //for event impacts
                    double max_damage1 = 0.0d;    // fired area (in acre)
                    double max_damage2 = 0.0d;    // fired properties (buildings or structures or residents)
                    double max_damage3 = 0.0d;    // cost of damage, e.g., $17 million, or $500 million - $1 billion
                    double max_magnitude1 = 0.0d;    // scale of 1-10: earthquakes, tsunamis, volcanic events
                    double max_magnitude2 = 0.0d;    // 1MinWinds 125: atlantic hurricane and pacific typhoon
                    double max_magnitude3 = 0.0d;    // pressure 907: atlantic hurricane and pacific typhoon
                    double max_fatalities = 0.0d;    // number of deaths

                    HashMap<String, Double> damage1_scores = new HashMap<String, Double>();
                    HashMap<String, Double> damage2_scores = new HashMap<String, Double>();
                    HashMap<String, Double> damage3_scores = new HashMap<String, Double>();
                    HashMap<String, Double> magnitude1_scores = new HashMap<String, Double>();
                    HashMap<String, Double> magnitude2_scores = new HashMap<String, Double>();
                    HashMap<String, Double> magnitude3_scores = new HashMap<String, Double>();
                    HashMap<String, Double> fatality_scores = new HashMap<String, Double>();

                    StringBuffer ltsb_raw = new StringBuffer();
                    ltsb_raw.append("Event").append("\t").
                            append("\t").append("DamagedArea").append("\t").
                            append("\t").append("DamagedProp").append("\t").
                            append("\t").append("DamagedCost").append("\t").
                            append("\t").append("Magnitude").append("\t").
                            append("\t").append("1MinWinds").append("\t").
                            append("\t").append("Pressure").append("\t").
                            append("\t").append("Fatalities").append("\t").append("\n");

                    //for (String s_event : event_idx.keySet()) {
                    // Extract R features for all other events occurred before the main event, i.e., all-pair comparison
                    // Note, 1) assume that the list are sorted descending by starting dates
                    for (int eid2 = eid + 1; eid2 < allEvents; eid2++) {
                        String s_event = all_events_list.get(eid2);

                        if (s_event.contains("hurricane_season")) continue;
                        if (event_map.get(s_event) == null) {
                            System.out.println("\tError: event_map.get(" + s_event + ") is null");
                            continue;
                        }

                        System.out.println("\tRelated event: [" + eid2 + "] " + s_event);

                        String damage_str = damage_map.get(s_event);
                        String fatality_str = fatality_map.get(s_event);
                        String magnitude_str = magnitude_map.get(s_event);

                        // Compute three impact factors: damage, magnitude, fatality
                        // (1) damage: floods, wildfires
                        double damage = 0.0d;
                        String damage_;

                        System.out.println("\t\t***damage_str = " + damage_str);
                        Matcher m;
                        int i = 0;

                        if (damage_str != null) {


                            m = p_damage1_2.matcher(damage_str);
                            i = 0;
                            boolean mass = false;
                            while (m.find()) {
                                System.out.println("\tDamage1-2[" + i + "]" + m.group());
                                damage_ = m.group().replaceAll("[^0-9.]", "");

                                if (damage_str.contains("million")) damage = Double.parseDouble(damage_) * 1000000;
                                else damage = Double.parseDouble(damage_);
                                damage1_scores.put(s_event, damage);
                                i++;
                                mass = true;
                            }

                            if (!mass) {
                                m = p_damage1_1.matcher(damage_str);

                                while (m.find()) {
                                    System.out.println("\tDamage1-1[" + i + "]" + m.group());
                                    damage_ = m.group().replaceAll("[^0-9.]", "");
                                    damage = Double.parseDouble(damage_);
                                    damage1_scores.put(s_event, damage);
                                    i++;
                                }
                            }

                            m = p_damage2.matcher(damage_str);
                            i = 0;
                            while (m.find()) {
                                System.out.println("\tDamage2[" + i + "]" + m.group());
                                damage_ = m.group().replaceAll("[^0-9.]", "");
                                damage = Double.parseDouble(damage_);
                                damage2_scores.put(s_event, damage);
                                i++;
                            }


                            m = p_damage3_1.matcher(damage_str);
                            i = 0;
                            boolean mass3 = false;
                            while (m.find()) {
                                System.out.println("\tDamage3-2[" + i + "]" + m.group());
                                damage_ = m.group().replaceAll("[^0-9.]", "");
                                if (damage_str.contains("billion")) damage = Double.parseDouble(damage_) * 1000;
                                else damage = Double.parseDouble(damage_);
                                damage3_scores.put(s_event, damage);
                                i++;
                                mass3 = true;
                            }

                            if (!mass3) {
                                m = p_damage3.matcher(damage_str);

                                while (m.find()) {
                                    System.out.println("\tDamage3-1[" + i + "]" + m.group());
                                    damage_ = m.group().replaceAll("[^0-9.]", "");
                                    damage = (double) Double.parseDouble(damage_) / 1000000;
                                    damage3_scores.put(s_event, damage);
                                    i++;
                                }
                            }
                        }

                        double magnitude = 0.0d;
                        String magnitude_;
                        if (magnitude_str != null) {
                            System.out.println("\t\t***magnitude_str = " + magnitude_str);

                            m = p_magnitude1.matcher(magnitude_str);
                            i = 0;
                            while (m.find()) {
                                System.out.println("\tMagnitude1[" + i + "]" + m.group());
                                magnitude_ = m.group().replace("1MinWinds", "");
                                magnitude_ = magnitude_.replaceAll("[^0-9.]", "");
                                magnitude = (double) Double.parseDouble(magnitude_);
                                magnitude1_scores.put(s_event, magnitude);
                                i++;
                            }

                            m = p_magnitude2.matcher(magnitude_str);
                            i = 0;
                            while (m.find()) {
                                System.out.println("\tMagnitude2[" + i + "]" + m.group());
                                magnitude_ = m.group().replace("1MinWinds", "");
                                magnitude_ = magnitude_.replaceAll("[^0-9.]", "");
                                magnitude = (double) Double.parseDouble(magnitude_);
                                magnitude2_scores.put(s_event, magnitude);
                                i++;
                            }

                            m = p_magnitude3.matcher(magnitude_str);
                            i = 0;
                            while (m.find()) {
                                System.out.println("\tMagnitude3[" + i + "]" + m.group());
                                magnitude_ = m.group().replace("1MinWinds", "");
                                magnitude_ = magnitude_.replaceAll("[^0-9.]", "");
                                magnitude = (double) Double.parseDouble(magnitude_);
                                magnitude3_scores.put(s_event, magnitude);
                                i++;
                            }
                        }


                        System.out.println("\t\t***fatality_str = " + fatality_str);

                        double fatality = 0.0d;
                        String fatality_;
                        if (fatality_str != null) {
                            m = p_fatality.matcher(fatality_str);
                            i = 0;
                            while (m.find()) {
                                System.out.println("\tFatality[" + i + "]" + m.group());
                                fatality_ = m.group().replaceAll("[^0-9.]", "");
                                fatality = (double) Double.parseDouble(fatality_);
                                fatality_scores.put(s_event, fatality);
                                i++;

                            }
                        }

                    }

                    System.out.println();

                    // Normalize time scores
                    for (int eid2 = eid + 1; eid2 < allEvents; eid2++) {
                        String s_event = all_events_list.get(eid2);
                        if (event_map.get(s_event) == null) {
                            System.out.println("\tError: event_map.get(" + s_event + ") is null");
                            continue;
                        }

                        double damage1 = damage1_scores.containsKey(s_event) ? damage1_scores.get(s_event) : Double.NaN;
                        double damage2 = damage2_scores.containsKey(s_event) ? damage2_scores.get(s_event) : Double.NaN;
                        double damage3 = damage3_scores.containsKey(s_event) ? damage3_scores.get(s_event) : Double.NaN;
                        double magnitude1 = magnitude1_scores.containsKey(s_event) ? magnitude1_scores.get(s_event) : Double.NaN;
                        double magnitude2 = magnitude2_scores.containsKey(s_event) ? magnitude2_scores.get(s_event) : Double.NaN;
                        double magnitude3 = magnitude3_scores.containsKey(s_event) ? magnitude3_scores.get(s_event) : Double.NaN;
                        double fatalities = fatality_scores.containsKey(s_event) ? fatality_scores.get(s_event) : Double.NaN;

                        // output scores
                        ltsb_raw.append(s_event).append("\t").
                                append(damage1).append("\t").append(damage2).append("\t").append(damage3).append("\t")
                                .append(magnitude1).append("\t").append(magnitude2).append("\t").append(magnitude3).append("\t").append(fatalities).append("\t").append("\n");
                    }

                    String outputFile_raw = event + "_impact_sim.csv";

                    String outputDir_lt = category + "/";
                    File outputF = new File(outputDir_lt);
                    if (!outputF.exists()) {
                        outputF.mkdirs();
                    }

                    try {
                        BufferedWriter bwr_raw = new BufferedWriter(new FileWriter(new File(outputDir_lt + outputFile_raw)));
                        bwr_raw.write(ltsb_raw.toString());
                        bwr_raw.flush();
                        bwr_raw.close();
                    } catch (FileNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        }

        return;
    }

    /**
     * Load event metadata from csv file
     * sort by starting date (ascending)
     * <p>
     * 15.12.13: Note this method is slightly different to the other classes that it also loads event impacts.
     *
     * @param metadataFilePath
     * @return
     */
    public TreeMap<String, EventPeriod> getEventMetadata(String metadataFilePath, HashMap<String, EventPeriod> expr_map,
                                                         HashMap<String, String> damage_map,
                                                         HashMap<String, String> fatality_map,
                                                         HashMap<String, String> magnitude_map) {
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
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(metadataFilePath), "UTF8"));
            String line = "";
            int st_i = 0;
            int et_i = 0;

            int d = -1;
            int f = -1;
            int m = -1;

            int lineCount = 0;
            while ((line = br.readLine()) != null) {
                String[] entry = line.split("\t");

                if (line.startsWith("#")) {
                    continue;
                }

                if (lineCount == 0) {
                    for (int i = 0; i < entry.length; i++) {
                        if (entry[i].equals(startingDate)) {
                            st_i = i;
                        }

                        if (entry[i].equals(endingDate)) {
                            et_i = i;
                        }

                        if (entry[i].equals(damage)) {
                            d = i;
                        }

                        if (entry[i].equals(magnitude)) {
                            m = i;
                        }

                        if (entry[i].equals(fatalities)) {
                            f = i;
                        }
                    }
                } else {
                    // I. Handle noisy annotated dates
                    // assume date format: yyyy-mm-dd
                    String cleaned_st = entry[st_i];
                    if (cleaned_st.length() > 10) {
                        cleaned_st = cleaned_st.replaceAll("\\s+", "");
                    }
                    String cleaned_et = entry[et_i];
                    if (cleaned_et.length() > 10) {
                        cleaned_et = cleaned_et.replaceAll("\\s+", "");
                    }
                    System.out.println("[" + lineCount + "] " + entry[0] + "\t" + cleaned_st + "\t" + cleaned_et);

                    LocalDate st = binner.parseDate(cleaned_st);
                    LocalDate et = binner.parseDate(cleaned_et);

                    // II. Handle wrong annotated dates
                    // 1) et_year must be greater than st_year
                    // E.g., 2012_Pakistan_floods st[2012-09-05], et[2010-09-09]
                    if (et.getYear() < st.getYear()) {
                        et = binner.parseDate(st.getYear() + cleaned_st.substring(cleaned_st.indexOf("-")));
                        System.out.println("\t1. new ending date" + et.toString());
                    }
                    // 2) [et_year = st_year]: et_month must be greater than st_month
                    else if (et.getYear() == st.getYear()) {

                        if (et.getMonthOfYear() < st.getMonthOfYear()) {

                            int m_gap = Months.monthsBetween(et, st).getMonths();

                            // use st_month
                            if (et.getDayOfMonth() > st.getDayOfMonth()) {
                                et = et.plusMonths(m_gap);
                                System.out.println("\t2. new ending date" + et.toString());
                            }
                            // use st_month + 1
                            else if (et.getDayOfMonth() < st.getDayOfMonth()) {
                                et = et.plusMonths(m_gap + 1);
                                System.out.println("\t3. new ending date" + et.toString());
                            }
                        }
                        // 3) Wrong date in WIkipedia infobox: no correct month is given
                        // 1998_Katherine_floods  st[1998-02-25], et[1998-02-03]
                        else if (et.getMonthOfYear() == st.getMonthOfYear()) {

                            // use the previous month for st
                            if (et.getDayOfMonth() < st.getDayOfMonth()) {
                                st = st.minusMonths(1);
                                System.out.println("\t4. new ending date" + et.toString());
                            }
                        }
                    }
                    String event = entry[0];
                    expr_map.put(event, new EventPeriod(st, et));

                    // add impact factors
                    if (d != -1) {
                        String damage = entry[d];
                        System.out.println("Damage: " + damage);
                        damage_map.put(event, damage);
                    }

                    if (m != -1) {
                        String magnitude = entry[m];
                        magnitude_map.put(event, magnitude);
                    }

                    if (f != -1) {
                        String fatality = entry[f];
                        fatality_map.put(event, fatality);
                    }
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
        MapValueComparator bvc = new MapValueComparator(expr_map);
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

    private String getEventType(String category) {

        String type = "";

        if (category.equals("atlantic_hurricane_season")) {
            type = "u";
        }
        if (category.equals("aviation_accidents_and_incidents")) {
            type = "u";
        }
        if (category.equals("civil_wars")) {
            type = "u";
        }
        if (category.equals("conferences")) {
            type = "p";
        }
        if (category.equals("cricket_world_cup")) {
            type = "p";
        }
        if (category.equals("diplomatic_conferences")) {
            type = "p";
        }
        if (category.equals("earthquakes")) {
            type = "u";
        }
        if (category.equals("explosions")) {
            type = "u";
        }
        if (category.equals("fifa_world_cup")) {
            type = "p";
        }
        if (category.equals("floods")) {
            type = "u";
        }
        if (category.equals("industrial_disasters")) {
            type = "u";
        }
        if (category.equals("mass_murder_in")) {
            type = "u";
        }
        if (category.equals("olympic_games")) {
            type = "p";
        }
        if (category.equals("pacific_typhoon_season")) {
            type = "u";
        }
        if (category.equals("protests")) {
            type = "u";
        }
        if (category.equals("revolutions")) {
            type = "u";
        }
        if (category.equals("riots_and_civil_disorder")) {
            type = "u";
        }
        if (category.equals("suicide_bombings")) {
            type = "u";
        }
        if (category.equals("terrorist_incidents")) {
            type = "u";
        }
        if (category.equals("tsunamis")) {
            type = "u";
        }
        if (category.equals("uefa_champions_league")) {
            type = "p";
        }
        if (category.equals("volcanic_events")) {
            type = "u";
        }
        if (category.equals("wildfires")) {
            type = "u";
        }


        return type;
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
}




