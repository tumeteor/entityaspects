package de.l3s.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.rosuda.JRI.REXP;

import l3s.de.event.features.QueryLogFeatures;
import l3s.de.event.features.RFeatures;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Files;


public class QueryClustering {
	
	public static String[] dynamic_queries_aol = {
		//"xbox",
		//"apple",
		//"ncaa",
		"kentucky_derby",
		"tigers",
		"tiger",
		"xmen",
		"harry_potter",
		"iraq_war",
		"leopard",
		//"windows",
		//"cricket",
		//"mlb",
		//"final_four",
		//"easter",
		"united_93",
		"ipod",
		"oscar",
		"da_vinci_code",
		"flight_93",
		"nba",
		"nasdaq",
		"star_wars",
		"final_fantasy",
		"eva",
		"uma_thurman",
		"nit",
		"nascar",
		"anna_nicole",
		"nfl",
		"nextel", //2006 NASCAR nextel cup
		"american_idol",
		"world_cup",
		"warped_tour",
		"graduation",
		"fifa",
		"indian wells",
		"kentucky",
		"newport",
		"olympics",
		"playstation",
		"jaguar",
		"virus",
		"tuscany",
		"preakness",
		"vista",
		"katrina",
		"acm_awards",
		"cma_awards", // not to be confused with acm
		"red_sox",
		"mountain_lion",
		"march_madness",
		"java",
		"world_poker_tour",
		"resident_evil",
		"spiderman",
		"google_maps",
		"cinco"	
	};

	
	static QueryLogFeatures ql = new QueryLogFeatures();

	public static void main_ (String[] args) throws IOException {
//		System.setProperty("R_HOME", "/Library/Frameworks/R.framework/Resources");

		RFeatures eval = new RFeatures();
		for (String query : dynamic_queries_aol) {
			File inputDir = new File("/Users/Admin/Work/entity research/event-learner/src/main/resources/implicit_1/" + query + "/");

			if(inputDir.isDirectory()){
	        	File[] dirs = inputDir.listFiles();
	        	for (File file : dirs) {
	        		System.out.println(file.getName());
	        		String fileName = file.getName();
	        		fileName = fileName.replace("rwrw_ranking_" + query + "-fix_equal_k_50_", "");
	                fileName = fileName.replace("_aol.dat", "");
	                String sDate = fileName.substring(0, 10);
	                String eDate = fileName.substring(11, fileName.length());
	                System.out.println(sDate + " " + eDate);
	        		List<String> queries = Files.readLines(file, Charsets.UTF_8);
	        		List<String> queries_ = new ArrayList<String>();
	        		for (String q : queries) {
	        			String[] q_ = q.split("\t");
	        			queries_.add(q_[1]);
	        		}
//	        		int[] a = { 7, 10, 20, 24, 30, 35, 40, 51, 57, 58, 71, 79, 81, 84, 94, 119, 121, 130, 135, 146, 147, 153, 163, 
//	        				   164, 198, 201, 206};
//	        		for (int i : a) {
//	        			System.out.println(queries_.get(i-1));
//	        		}
	        		double[][] a  = new double[queries_.size()][queries_.size()];
	        		for (int i = 0; i < queries_.size(); i++) {
	        			for (int j = 0; j < queries_.size(); j ++) {
	        				if (i == j) a[i][j] = 1;
	        				else {
	        					double sim_edit = QueryClustering.simEdit(queries_.get(i), queries_.get(j));
	        					double sim_keyword = QueryClustering.simKeyWords(queries_.get(i), queries_.get(j));
	        					double sim_coclick = QueryClustering.simCoClick(queries_.get(i), queries_.get(j), sDate, eDate);
	        					double sim_semantic = QueryClustering.simSemantic(queries_.get(i), queries_.get(j));
	        					a[i][j] = max(sim_edit, sim_keyword, sim_coclick, sim_semantic);
	        					/*
	        					if (a[i][j] == 1.0) System.out.println(queries_.get(i) + " : " + queries_.get(j) + " " + 
	        							sim_edit + " " + sim_keyword + " " + sim_coclick + " " + sim_semantic);
	        					*/
	        				}	
	        			}
	        		}
	        		eval.engine.eval("sim_matrix = matrix(data = NA, nrow = " + a.length + ", ncol = " + a.length + ", byrow = FALSE, dimnames = NULL)");
	        		//loop through the matrix and give the upgma_matrix the correct values
	                for(int i = 0; i< a.length; i++) {
	                	eval.engine.eval("i = " + i);
	                	for (int j = 0; j < a.length; j++) {
	                		eval.engine.eval("j = " + j);
	                		//R matrices start at index 1 (java at 0), so add 1 to current position
	                    	eval.engine.eval("ii = i + 1");
	                    	eval.engine.eval("jj = j + 1");
	                    	//add values for the lower triangle..
	                    	eval.engine.eval("sim_matrix [ii,jj] = " + a[i][j]);
	                	}
	                	
	                }
	        		//get the names of the accessions from the [] rows array
	        		eval.engine.eval("row.names(sim_matrix) = " + QueryClustering.arrayToString(queries_.toArray(new String[queries_.size()])));
	        		eval.engine.eval("colnames(sim_matrix) = " + QueryClustering.arrayToString(queries_.toArray(new String[queries_.size()])));

	        		//make sure it's in the right format
	        		eval.engine.eval("sim_matrix <- as.matrix (sim_matrix)");

//	        		eval.engine.eval("show(sim_matrix)");
//	        		eval.engine.eval("sim_matrix <- as.dist (sim_matrix)");
	        		//run clustering on the matrix
//	        		eval.engine.eval("clus_out <- hclust(sim_matrix)");
//	        		REXP rexp = eval.engine.eval ("clus_out$merge");
//	        		double [][] returnedMatrix = rexp.asDoubleMatrix ();
//	        		REXP rowNames = eval.engine.eval ("clus_out$labels");
//	        		String [] rowNamesArray = rowNames.asStringArray ();
//	        		System.out.println(Arrays.toString(rowNamesArray));
	        		
	        		
	        		eval.engine.eval("apres = apcluster(sim_matrix)");
	        		eval.engine.eval ("sink(\"/Users/Admin/Work/entity research/data/subtopics/" + file.getName() + "\")");
	        		eval.engine.eval ("show(apres)");
	        		eval.engine.eval("sink()");
	        	}
			}
		}
		
	} 
	
	public static void main (String[] args) {
		System.out.println(QueryClustering.simSemantic("Berlin", "Hannover"));
		System.out.println(QueryClustering.simSemantic("Berlin", "Hanoi"));
	}

	
	public static String arrayToString(String[] queries) {
		StringBuffer s = new StringBuffer();
		s.append("c(");
		int i = 0;
		for (String q : queries) {
			s.append("\"").append(q);
			if(i++ < queries.length - 1) s.append("\",");
			else s.append("\"");
		}
		s.append(")");
		return s.toString();
	}
	
	public static double simEdit(String q, String p) {
		double score = 0.0d;
		score = 1 - (double) editDistance(q, p)/ Math.max(q.length(), p.length());
		return score;
	}
	
	public static double simKeyWords(String q, String p) {
		Set<String> q_ = Sets.newHashSet(q.split(" "));
		Set<String> p_ = Sets.newHashSet(p.split(" "));
		// intersection  
		Set<String> intersect = new HashSet<String>(q_);  
		intersect.retainAll(p_);
		double score = 0.0d;
		score = (double) intersect.size()/Math.max(q_.size(), p_.size());
		return score;
	}
	
	public static double simCoClick(String q, String p, String sDate, String eDate) { 
		double score = 0.0d;
		Set<String> q_ = new HashSet<String>(ql.getClickedURLs(q, sDate, eDate));
		Set<String> p_ = new HashSet<String>(ql.getClickedURLs(p, sDate, eDate));
		// intersection  
		Set<String> intersect = new HashSet<String>(q_);  
		intersect.retainAll(p_);
		System.out.println(intersect.size() + " " + Math.max(q_.size(), p_.size()));
		score = (double) intersect.size()/Math.max(q_.size(), p_.size());
		return score;
	}
	
	public static double simSemantic(String q, String p) {
		double score = 0.0d;
		String[] q_ = q.split(" ");
		String[] p_ = p.split(" ");

		double[][] a = WordNetSimCal.getNormalizedSimilarityMatrix(q_, p_);
		for (int i = 0; i < a.length; i ++) {
			for (int j  = 0; j < a[i].length; j++) {
				score += a[i][j];
			}
			score = score / a[i].length;
		}
		score = (double) score / a.length;
		return score;
	}
	
	public static int editDistance(String s, String t) {
		int m = s.length();
		int n = t.length();
		int[][] d = new int[m+1][n+1];
		for(int i = 0;i <= m;i++){
			d[i][0] = i;
		}
		for  (int j = 0;j <= n;j++) {
			d[0][j] = j;
		}
		for(int j = 1;j <= n;j++) {
			for(int i = 1;i <= m;i++){
				if(s.charAt(i-1) == t.charAt(j-1)) {
					d[i][j] = d[i-1][j-1];
				}
				else{
					d[i][j] = min((d[i-1][j]+1),(d[i][j-1]+1),(d[i-1][j-1]+1));
				}
			}
		}
		return(d[m][n]);
	}
	
	public static int min(int a,int b,int c) {
		return(Math.min(Math.min(a,b),c));
	}
	
	public static double max(double a, double b, double c, double d) {
		return (Math.max(Math.max(Math.max(a, b), c),d));
		
	}
	

}
