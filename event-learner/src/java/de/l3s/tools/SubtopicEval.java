package de.l3s.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import l3s.de.event.features.QueryLogFeatures;

public class SubtopicEval {
	static QueryLogFeatures ql = new QueryLogFeatures();
	
	public static void main(String[] args) {
		File inputDir = new File("/Users/Admin/Work/entity research/data/subtopics");
		if(inputDir.isDirectory()){
			//"/Users/Admin/Work/entity research/data/subtopics/rwrw_ranking_final_four-fix_equal_k_50_2006-02-28_2006-04-07_aol.dat"
        	File[] dirs = inputDir.listFiles();
        
        	DescriptiveStatistics coh = new DescriptiveStatistics();
        	DescriptiveStatistics plau = new DescriptiveStatistics();
        	DescriptiveStatistics dist = new DescriptiveStatistics();
        	DescriptiveStatistics complete = new DescriptiveStatistics();
        	double[] betas = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
        	for (double beta : betas) {
        		for (File file : dirs) {
            		String filename = file.getName();
            		filename = filename.replaceFirst("rwrw_ranking_", "");
            		filename = filename.substring(0, filename.indexOf("-"));
            		//System.out.println(filename);
            		List<String> subtopics = getSubtopicsFromFile(file);
            		if (subtopics.size() == 0) continue;
            		double c = 0.0d;
            		double p = 0.0d;
            		for (String s : subtopics) {
            			//c += getCoherence(s);
            		    p += getPlausible(filename, s, beta);
            		}
        			//coh.addValue((double) c / subtopics.size());
        			plau.addValue((double) p / subtopics.size());
        			double ct = getCompleteness(filename, subtopics, beta);
            		complete.addValue(ct);
            		//double d = getDistinctness(subtopics.toArray(new String[subtopics.size()]));
            		//dist.addValue(d);

            	}
            	//System.out.println("coherence: " + coh.getMean() + coh.getStandardDeviation() + coh.getKurtosis());
            	//System.out.println("distinctness: " + dist.getMean() + dist.getStandardDeviation() + dist.getKurtosis());
            	System.out.println("plausible: " + beta + " " + plau.getMean() + " " + plau.getStandardDeviation() + " " + plau.getKurtosis());
            	System.out.println("complete: " + beta + " " + complete.getMean() + " " +  complete.getStandardDeviation() + " " + complete.getKurtosis());
            	
            	System.out.println("========================");
        	}
    		
		}

	}

	public static double getJaccardSim(ArrayList<String> q, ArrayList<String> p) {
		Set<String> q_ = new HashSet<String>(q);
		Set<String> p_ = new HashSet<String>(p);
		// intersection  
		Set<String> intersect = new HashSet<String>(q_);  
		intersect.retainAll(p_);
		double score = (double) 2 * intersect.size() / (q_.size() + p_.size());
		return score;
		
	}
	
	/**
	 * coherence for a query 
	 * a subtopic i is alpha-coherence if 2 different set of judged docs R1, R2
	 * sim(R1,R2) > alpha
	 * @param s subtopic
	 * @return
	 */
	public static double getCoherence(String s) {
		HashMap<String, ArrayList<String>> su_map = ql.getClickedURLsBySession(s);
		Set<String> keys = su_map.keySet();
		double coh = 0.0d;
		int pairs = 0;
		if(keys.size() == 1) return 0.0d;
		for (int i = 0; i < keys.size(); i++) {
			for (int j = 0; j < i; j++) {
				String[] ks = keys.toArray(new String[keys.size()]);
				coh += getJaccardSim(su_map.get(ks[i]), su_map.get(ks[j]));
				pairs ++;
			}
		}
		double coherence = (pairs == 0) ? 0.0d : (double) coh / pairs;
		return coherence;
	}
	
	/**
	 * distinctness for a query's list of subtopics
	 * 2 subtopics i1, i2 are alpha-distinct if 
	 * sim(i1,i2) <= 1 - alpha
	 * @param subtopics
	 * @return
	 */
	public static double getDistinctness(String[] subtopics) {
		double dist = 0.0d;
		int pairs = 0;
		for (int i = 0; i < subtopics.length; i++) {
			for (int j = 0; j < i; j++) {
				dist += getJaccardSim(ql.getClickedURLs(subtopics[i]), ql.getClickedURLs(subtopics[j]));
				pairs ++;
			}
		}
		return (pairs == 0) ? 1.0d : 1.0d - (double)dist / pairs;
	}
	
	public static double getPlausible(String q, String s, double beta) {
		HashMap<String, ArrayList<String>> q_map = ql.getClickedURLsBySession(q);
		ArrayList<String> slist = ql.getClickedURLs(s);
		//HashMap<String, ArrayList<String>> s_map = ql.getClickedURLsBySession(s);
		Set<String> q_keys = q_map.keySet();
		//Set<String> s_keys = s_map.keySet();
		String[] qk = q_keys.toArray(new String[q_keys.size()]);
		//String[] sk = s_keys.toArray(new String[s_keys.size()]);
		int count = 0;
		for (int i = 0; i < q_keys.size(); i++) {
			//for (int j = 0; j < s_keys.size(); j++) {
			double js = getJaccardSim(q_map.get(qk[i]), slist);
			if (js > beta) count ++;
		}

		double plau = (q_keys.size() == 0) ? 0.0d : (double) count / q_keys.size();
		return plau;


	}
	
	public static double getCompleteness(String q, List<String> subtopics, double beta) {
		HashMap<String, ArrayList<String>> q_map = ql.getClickedURLsBySession(q);
		Set<String> q_keys = q_map.keySet();
		String[] qk = q_keys.toArray(new String[q_keys.size()]);
		int count = 0;
		for (int i = 0; i < q_keys.size(); i++) {
			double max_js = 0.0d;
			for (String s : subtopics) {
				//HashMap<String, ArrayList<String>> s_map = ql.getClickedURLsBySession(s);
				//Set<String> s_keys = s_map.keySet();
				//String[] sk = s_keys.toArray(new String[s_keys.size()]);
				//for (int j = 0; j < s_keys.size(); j++) {
				double js = getJaccardSim(q_map.get(qk[i]), ql.getClickedURLs(s));
				if (max_js < js) max_js = js;
			}

			if (max_js > beta) count++;
		}

		double complete = (q_keys.size() == 0) ? 0.0d : (double) count / q_keys.size();
		return complete;
	}


	public static List<String> getSubtopicsFromFile(File file) {
		ArrayList<String> subtopics = new ArrayList<String>();
		try {
			List<String> lines = Files.readLines(file, Charsets.UTF_8);
			for (String line : lines) {
				if (line.startsWith("   Cluster")) {
					line = line.replace(":", "");
					String[] line_ = line.split("\\,");
					String subtopic = line_[1].replace(" exemplar ", "");
					subtopics.add(subtopic);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return subtopics;
	}
	
	
	

}
