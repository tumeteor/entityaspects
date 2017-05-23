package de.l3s.tools;

import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.JiangConrath;
import edu.cmu.lti.ws4j.util.WS4JConfiguration;

public class WordNetSimCal {
	
	private static ILexicalDatabase db = new NictWordNet();
	private static RelatednessCalculator rc = new JiangConrath(db);
	public static double sim( String word1, String word2 ) {
		WS4JConfiguration.getInstance().setMFS(true);
		double s = rc.calcRelatednessOfWords(word1, word2);
		return s;
	}
	public static void main(String[] args) {
		long t0 = System.currentTimeMillis();
		System.out.println(sim( "berlin","paris"));
		long t1 = System.currentTimeMillis();
		System.out.println( "Done in "+(t1-t0)+" msec." );
	}
	public static double[][] getSimilarityMatrix( String[] words1, String[] words2) {
		double[][] result = new double[words1.length][words2.length];
		for ( int i=0; i<words1.length; i++ ) {
			for ( int j=0; j<words2.length; j++ ) {
				double score = rc.calcRelatednessOfWords(words1[i], words2[j]);
				result[i][j] = score;
			}
		}
		return result;
	}
	
	public static double[][] getNormalizedSimilarityMatrix( 
			String[] words1, String[] words2 ) {
		double[][] scores = getSimilarityMatrix( words1, words2);
		double bestScore = 1; // normalize if max is above 1
		for ( int i=0; i<scores.length; i++ ) {
			for ( int j=0; j<scores[i].length; j++ ) {
				if ( scores[i][j] > bestScore && scores[i][j] != Double.MAX_VALUE ) {
					bestScore = scores[i][j];
				}
			}
		}
		
		for ( int i=0; i<scores.length; i++ ) {
			for ( int j=0; j<scores[i].length; j++ ) {
				
				if ( scores[i][j] == Double.MAX_VALUE ) {
					scores[i][j] = 1;
				} else {
					scores[i][j] /= bestScore;
				}
			}
		}
		return scores;
	}
	
}
