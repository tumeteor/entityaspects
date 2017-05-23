package de.l3s.common;

import java.util.ArrayList;
import java.util.List;

public class WeightedJaccard {
	
	public static double jaccardSimilarity(List<?> d1, List<?> d2) {
		int overlap = 0;
		List<?> shortd, longd;
		if (d1.size() > d2.size()) {
			shortd = d2;
			longd = d1;
		} else {
			shortd = d1;
			longd = d2;
		}
		for (Object d : shortd) {
			if (longd.contains(d)) {
				overlap++;
			}
		}
		return (overlap * 2.) / (d1.size() + d2.size());
	}
	
	public static int editDistance(String s, String t) {
		int m = s.length();
		int n = t.length();
		int[][] d = new int[m + 1][n + 1];
		for (int i = 0; i <= m; i++) {
			d[i][0] = i;
		}
		for (int j = 0; j <= n; j++) {
			d[0][j] = j;
		}
		for (int j = 1; j <= n; j++) {
			for (int i = 1; i <= m; i++) {
				if (s.charAt(i - 1) == t.charAt(j - 1)) {
					d[i][j] = d[i - 1][j - 1];
				} else {
					d[i][j] = min((d[i - 1][j] + 1), (d[i][j - 1] + 1),
							(d[i - 1][j - 1] + 1));
				}
			}
		}
		return (d[m][n]);
	}

	public static int min(int a, int b, int c) {
		return (Math.min(Math.min(a, b), c));
	}
	
	public static void main (String[] args) {
		List<String> a = new ArrayList<String>();
		a.add("a");
		a.add("a");
		a.add("a");
		a.add("b");
		a.add("c");
		List<String> b = new ArrayList<String>();
		b.add("a");
		b.add("a");
		b.add("a");
		b.add("b");
		
		System.out.println(jaccardSimilarity(a, b));
		
	}
}
