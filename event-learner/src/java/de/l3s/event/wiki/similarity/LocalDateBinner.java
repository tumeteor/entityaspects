package l3s.de.event.wiki.similarity;

import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;


public class LocalDateBinner implements BinningStrategy<LocalDate> {
    private final static DateTimeFormatter inputFormatter = new DateTimeFormatterBuilder()
            .appendYear(4, 4).appendMonthOfYear(2).appendDayOfMonth(2)
            .toFormatter();
    private final DateTimeFormatter dateFormat = DateTimeFormat
            .forPattern("yyyy-MM-dd");

    public static void main(String[] args) {
        LocalDateBinner binner = new LocalDateBinner();
//		System.out.println(binner.parseDate_("2008-04-02").toDateTime(
//				new DateTime(0, 12, 31, 23, 59)));
        System.out.println(binner.parseDate("2008-04-02").compareTo(binner.parseDate("2008-04-01")));
    }

    @Override
    public LocalDate binToDate(final LocalDate first, final int index) {
        return first.plusDays(index);
    }

    @Override
    public int getBin(final LocalDate first, final String current) {
        return getBin(first, parseDate(current));
    }

    @Override
    public int getBin(final LocalDate first, final LocalDate current) {
        return Days.daysBetween(first, current).getDays();
    }

    @Override
    public DateTimeFieldType getBinningDateField() {
        return DateTimeFieldType.dayOfYear();
    }

    @Override
    public Class<LocalDate> getDateClass() {
        return LocalDate.class;
    }

    public LocalDate parseDate_(final String s) {
        return inputFormatter.withZone(DateTimeZone.UTC).parseDateTime(s)
                .toLocalDate();
    }

    @Override
    public LocalDate parseDate(String s) {
        return LocalDate.parse(s, dateFormat);
    }

    public LocalDate parseDate(String s, DateTimeFormatter dateFormat) {
        return LocalDate.parse(s, dateFormat);
    }
}