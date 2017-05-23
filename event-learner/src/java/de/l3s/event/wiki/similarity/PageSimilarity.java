package l3s.de.event.wiki.similarity;

import org.joda.time.DateMidnight;
import org.joda.time.Days;
import org.joda.time.LocalDate;

import de.l3s.tools.LocalDateBinner;

public class PageSimilarity {
	public double lambda = 0.5;
	public double decayRate = 0.5;

	LocalDateBinner binner = new LocalDateBinner();
	
	

	/**
	 * 
	 * @param s1
	 * @param e1
	 * @param s2
	 * @param e2
	 * @param ts frequency array of the event
	 * @return
	 */
	public double TSUSim (LocalDate s1, LocalDate e1, LocalDate s2, LocalDate e2) {
		double s_decay = 0.0d; 
		double e_decay = 0.0d; 
		DateMidnight st_1 = new DateMidnight(s1.toString());
		DateMidnight st_2 = new DateMidnight(s2.toString());
		DateMidnight et_1 = new DateMidnight(e1.toString());
		DateMidnight et_2 = new DateMidnight(e2.toString());
		
		int sGap = Days.daysBetween(st_1, st_2).getDays();
		int eGap = Days.daysBetween(et_1, et_2).getDays();
		
//		int sGap = Months.monthsBetween(st_1, st_2).getMonths();
//		int eGap = Months.monthsBetween(et_1, et_2).getMonths();
		
//		int sGap = Years.yearsBetween(st_1, st_2).getYears()/10;
//		int eGap = Years.yearsBetween(et_1, et_2).getYears()/10;
		
		if (sGap >= 0) s_decay = Math.pow(decayRate,(lambda * sGap));
		if (eGap >= 0) e_decay = (eGap != 0) ? Math.pow(decayRate,(lambda*eGap)) : 0;

		return 0.5 * (s_decay + e_decay);
		
	}

}
