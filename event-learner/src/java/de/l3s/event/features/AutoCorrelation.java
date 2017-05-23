package l3s.de.event.features;


import java.util.Arrays;
import java.util.Enumeration;

import l3s.de.rjava.TextConsole;

import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RVector;
import org.rosuda.JRI.Rengine;


public class AutoCorrelation {
	Rengine engine;
	public AutoCorrelation(){
		String[] rOptions = { "--vanilla" };
		engine = new Rengine(rOptions, false, new TextConsole());
	}
    /**
     * 
     * @param lengthOfPeriod
     * @param timeSeries
     * @return
     */
	public double[] computeAutoCorrel(int lengthOfPeriod, int[] timeSeries) {
		engine.assign("data", timeSeries);
		REXP x = engine.eval("data = ts(data, frequency =" + lengthOfPeriod +")",false);
		x = engine.eval("query_acf = acf(data, lag.max = NULL, plot = FALSE, type = \"correlation\", na.action = na.pass)",false);
		x = engine.eval("query_acf$acf");
	    double[] scores = x.asDoubleArray();
	    System.out.println(Arrays.toString(scores));
	    return scores;
	}
	
	public void test(){
		engine.eval("x <- c(1,2,3,3,2,3,4,5,6)");
		REXP x = engine.eval("my_acf = acf(x,plot=F)");
		x = engine.eval("my_acf$acf");
	    double[] scores = x.asDoubleArray();
	    System.out.println(Arrays.toString(scores));
	}
	
	public void endEngine(){
		engine.end();
	}

	public static void main(String[] args) {
		AutoCorrelation eval = new AutoCorrelation();
		int[] sample = {-1, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1};
		int[] april_fool_ts = {2,3,2,1,0,5,1,0,6,0,0,0,5,0,3,10,3,11,4,1,5,2,7,2,8,4,37,9,9,20,169,337,22,2,9,3,0,3,1,7,0,0,1,1,1,2,0,3,2,2,0,0,1,0,3,1,0,2,0,3,0,1,0,1,0,0,0,3,1,0,0,6,0,0,0,3,0,0,1,0,1,0,0,3,0,0,0,0,0,0,0,0};
		int[] american_idol_ts = {359,581,470,178,67,88,173,442,533,505,153,190,122,262,825,510,135,94,68,144,257,694,479,207,192,110,113,209,643,482,105,55,85,71,165,681,463,139,74,83,60,194,345,240,120,59,34,65,214,696,465,70,61,71,59,270,648,450,120,56,84,71,206,762,514,125,89,89,129,266,1244,1149,384,198,169,226,412,12,538,196,137,118,188,393,716,421,159,85,50,97,51,33};
		eval.computeAutoCorrel(april_fool_ts.length, april_fool_ts);
		eval.computeAutoCorrel(american_idol_ts.length, american_idol_ts);
		eval.computeAutoCorrel(sample.length, sample);
		eval.test();
		eval.endEngine();

	}
}
