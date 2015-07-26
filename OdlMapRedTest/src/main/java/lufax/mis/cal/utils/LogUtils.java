package lufax.mis.cal.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogUtils {
	  private static final DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	  public static void log2Screen(String text) {
	    Date currentDate = new Date();

	    System.out.print(format.format(currentDate) + " ");
	    System.out.println(text);
	  }

	  public static void log2Stderr(String text) {
	    Date currentDate = new Date();

//	    System.err.println(format.format(currentDate) + " " + text);
	    System.err.print(format.format(currentDate) + " ");
	    System.err.println(text);
	  }

		public static void main(String args[]) throws Exception {
			String text = "123 handsome";
			LogUtils.log2Screen(text);
		}
}
