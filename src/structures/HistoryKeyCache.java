package structures;

import java.util.Arrays;
import java.util.Locale;

final class HistoryKeyCache {
    private static final int CAPACITY = 2048;
    private final long[] first = new long[CAPACITY];
    private final long[] second = new long[CAPACITY];
    private final long[] third = new long[CAPACITY];
    private final int[] lengths = new int[CAPACITY];
    private final String[] values = new String[CAPACITY];
    private Locale locale;

    String key(double[] vector) {
        Locale currentLocale = Locale.getDefault(Locale.Category.FORMAT);
        if (!currentLocale.equals(locale)) {
            Arrays.fill(values, null);
            locale = currentLocale;
        }
        int length = Math.min(3, vector.length);
        long a = length > 0 ? Double.doubleToLongBits(vector[0]) : 0;
        long b = length > 1 ? Double.doubleToLongBits(vector[1]) : 0;
        long c = length > 2 ? Double.doubleToLongBits(vector[2]) : 0;
        long hash = a ^ Long.rotateLeft(b, 21) ^ Long.rotateLeft(c, 42) ^ length;
        hash ^= hash >>> 33;
        hash *= 0xff51afd7ed558ccdL;
        hash ^= hash >>> 33;
        int slot = (int) hash & (CAPACITY - 1);
        if (values[slot] != null && lengths[slot] == length
                && first[slot] == a && second[slot] == b && third[slot] == c) {
            return values[slot];
        }

        // Keep Formatter's rounding, signed zero, non-finite values and locale exactly.
        // A collision merely replaces a cached entry; it never changes key equality.
        String value;
        switch (length) {
            case 3:
                value = String.format(currentLocale, "%.3f,%.3f,%.3f,", vector[0], vector[1], vector[2]);
                break;
            case 2:
                value = String.format(currentLocale, "%.3f,%.3f,", vector[0], vector[1]);
                break;
            case 1:
                value = String.format(currentLocale, "%.3f,", vector[0]);
                break;
            default:
                value = "";
        }
        first[slot] = a;
        second[slot] = b;
        third[slot] = c;
        lengths[slot] = length;
        values[slot] = value;
        return value;
    }
}
