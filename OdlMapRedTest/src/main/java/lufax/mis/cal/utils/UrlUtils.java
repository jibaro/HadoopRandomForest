package lufax.mis.cal.utils;

import lufax.mis.cal.common.MisCommon;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by lufax on 7/3/15.
 */
public class UrlUtils {
    public static Integer getUrlType(String urlString) throws MalformedURLException {
        URL url = new URL(urlString);

        String host = url.getHost();
        if (host.contains(".lufax.com") || host.contains(".lu.com")) {
            if (host.equals("perf.lufax.com") || host.equals("perf.lu.com")) {
                return MisCommon.PERF;
            } else {
                return MisCommon.CAL;
            }
        } else {
            return MisCommon.OTHER;
        }
    }

    public static void main(String[] args) throws MalformedURLException {
        String url = "https://perf.lu.com/img/behavior.gif?start-time%3D1435906309215%26rdm%3D…ta-type%3Dclick%26result_url%3Dhttp%3A%2F%2Flist.lufax.com%2Flist%2Fpiaoju";
        System.out.println(getUrlType(url));
    }

}
