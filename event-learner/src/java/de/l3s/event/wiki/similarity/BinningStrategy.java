package l3s.de.event.wiki.similarity;

import org.joda.time.DateTimeFieldType;
import org.joda.time.ReadablePartial;
import org.joda.time.format.DateTimeFormatter;

public interface BinningStrategy<T extends ReadablePartial> {

    public int getBin(T first, String current);

    public int getBin(T first, T current);

    public Class<T> getDateClass();

    public DateTimeFieldType getBinningDateField();
    
    public T parseDate(String s);
    
    public T parseDate(String s, DateTimeFormatter dateFormat);

    public T binToDate(T first, int index);
}
