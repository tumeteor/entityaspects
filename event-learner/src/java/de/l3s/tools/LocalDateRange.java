package de.l3s.tools;

import org.joda.time.LocalDate;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class LocalDateRange implements Iterable<LocalDate> {
    private final LocalDate start;
    private final LocalDate end;

    public LocalDateRange(LocalDate start, LocalDate end) {
        this.start = start;
        this.end = end;
    }

    public static void main(String args[]) {

    }

    @Override
    public Iterator<LocalDate> iterator() {
        return new LocalDateRangeIterator(start, end);
    }

    private static class LocalDateRangeIterator implements Iterator<LocalDate> {
        private final LocalDate end;
        private LocalDate current;

        private LocalDateRangeIterator(LocalDate start, LocalDate end) {
            this.current = start;
            this.end = end;
        }

        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public LocalDate next() {
            if (current == null) {
                throw new NoSuchElementException();
            }
            LocalDate ret = current;
            current = current.plusDays(1);
            if (current.compareTo(end) > 0) {
                current = null;
            }
            return ret;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
