package de.l3s.tools;

import org.joda.time.DateTimeFieldType;
import org.joda.time.Months;
import org.joda.time.YearMonth;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class YearMonthBinner implements BinningStrategy<YearMonth> {
	private final DateTimeFormatter dateFormat = DateTimeFormat
			.forPattern("YYYYMM");

    @Override
    public YearMonth binToDate(final YearMonth first, final int index) {
        return first.plusMonths(index);
    }

    @Override
    public int getBin(final YearMonth first, final String current) {
        return getBin(first, parseDate(current));
    }

    @Override
    public int getBin(final YearMonth first, final YearMonth current) {
        return Months.monthsBetween(first, current).getMonths();
    }

    @Override
    public DateTimeFieldType getBinningDateField() {
        return DateTimeFieldType.monthOfYear();
    }

    @Override
    public Class<YearMonth> getDateClass() {
        return YearMonth.class;
    }

    @Override
    public YearMonth parseDate(final String s) {
		return YearMonth.parse(s.substring(0, 6), dateFormat);
    }

	public static void main(String[] args) {
		YearMonthBinner binner = new YearMonthBinner();
		System.out.println(binner.parseDate("2008-04-01").toString());
	}

	@Override
	public YearMonth parseDate(String s, DateTimeFormatter dateFormat) {
		// TODO Auto-generated method stub
		return null;
	}
}