package lufax.mis.cal.utils;

import java.sql.Timestamp;    ;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class DateUtils {
	private static final DateFormat partitionDateFormat = new SimpleDateFormat("yyyyMMddHH");
    private static final DateFormat rdDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final DateFormat fullyDateFormat = new SimpleDateFormat("yyyyMMddHHmm");

	public static String getPartition(Date date) {
	  return partitionDateFormat.format(date);
	}


	public static String getPartition(Timestamp requestTime) {

	    return partitionDateFormat.format(requestTime.getTime());

	}

    public static Date parsePartition(String partition) throws ParseException {
        return partitionDateFormat.parse(partition);
    }

    public static String getRd(Date date) {
        return rdDateFormat.format(date);
    }

    public static Date parseRd(String rd) throws ParseException {
        return rdDateFormat.parse(rd);
    }

    public static Date parseFullyDate(String date) throws ParseException {
        return fullyDateFormat.parse(date);
    }

    public static Date parseDate(String dateStr) throws ParseException {

        if (dateStr.substring(1,10).contains("-")){
            return parsePartition(dateStr.substring(1,10));
        }
        throw new ParseException("Cannot regenize date string " + dateStr, 1);
    }

    public static void main(String[] args) throws ParseException {
        String requestTime = "2015-01-01 17:45:12.0";

        Date date = parseDate(requestTime);

        Timestamp ts = Timestamp.valueOf(requestTime);

        System.out.print(date);
        System.out.print("\n");

        System.out.print(ts.getTime());




    }

}
