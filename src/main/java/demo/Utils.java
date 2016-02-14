package demo;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Utils {

    private static DateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);

    public static boolean isInRange(String date, String after, String before) {
        try {
            Date d = sdf.parse(date);
            Date min = sdf.parse(after);
            Date max  = sdf.parse(before);

            return min.getTime() <= d.getTime() && d.getTime() <= max.getTime();
        } catch (ParseException ignored) {
            //log goes here
        }
        return false;
    }
}
