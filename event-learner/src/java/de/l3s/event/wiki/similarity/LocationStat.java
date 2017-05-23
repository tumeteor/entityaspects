package l3s.de.event.wiki.similarity;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;

import au.com.bytecode.opencsv.CSVReader;

public class LocationStat {


	public List<String> getCountries() {
		String[] countryCodes = Locale.getISOCountries();
	    List<String> countries = new ArrayList<String>(countryCodes.length);
	    for (String countryCode : countryCodes) {
	      countries.add(new Locale("", countryCode).getDisplayCountry(Locale.US));
	    }    
	   return countries;
	}


	public static void main (String[] args) {
		LocationStat ls = new LocationStat();
		List<String> countries = ls.getCountries();
	
		File inputFile = new File("/Users/Admin/Work/entity research/event-learner/src/main/resources/location_geo2/wildfires");	
		if(inputFile.exists() && inputFile.isDirectory()){

			File[] files = inputFile.listFiles();
			Arrays.sort(files);
			int noOfFiles = files.length;
			for(int f = 0; f<noOfFiles; f++){
				String event = files[f].getAbsoluteFile().getName();
				System.out.println(event);
				
				String category = "wildfires";
				HashMap<String, Integer> map = new HashMap<String, Integer>();
				CSVReader reader;
				try {
					reader = new CSVReader(new FileReader(files[f]), '\t');
					String[] entry;
					//exclude titles
					reader.readNext();
					String[] locs;
					while ((entry = reader.readNext()) != null) {
						entry[3] = entry[3].replaceAll("\\[", "");
						entry[3] = entry[3].replaceAll("\\]", "");
						locs = entry[3].split(",");
						
						for (String loc : locs) {
							if (countries.contains(loc)) {
								if (map.containsKey(loc)) {
									map.put(loc, map.get(loc) + 1);
								}
								else {
									map.put(loc, 1);
								}
							}
						}

					}

				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
				
				StringBuffer sb = new StringBuffer();
				
				for (Entry<String,Integer> e : map.entrySet()) {
					if (e.getValue() > 0) {
						System.out.println(e.getValue());
						sb.append(e.getKey()).append("\t").append(e.getValue());
						sb.append("\n");
					}
				}
		
				String outputFile_raw =  event + "_impact_sim.csv";
                
				String outputDir_lt = "loc_stat/" + category + "/";
				File outputF = new File(outputDir_lt);
				if(!outputF.exists()) {
					outputF.mkdirs();
				}

				try {
					BufferedWriter bwr_raw = new BufferedWriter(new FileWriter(new File(outputDir_lt +  outputFile_raw)));
                    bwr_raw.write(sb.toString());
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

}
