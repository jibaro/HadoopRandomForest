package lufax.mis.cal.utils;

import ua_parser2.Client;
import ua_parser2.Parser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by geyanhao801 on 7/20/15.
 */
public class UserAgentUtils {

    public UserAgentUtils(){
        return;
    }

    public static Client uaParse(String str) throws IOException{
        InputStream inputStream = FileInputStream.class.getResourceAsStream("/regexes.yaml");
        System.out.println(inputStream.toString());
        inputStream.close();
        Parser uaParser = new Parser();
        Client c = uaParser.parse(str);
        return c;

    }

    public static void main(String[] args) throws IOException{
        String ua = "Mozilla/5.0 (X11; Linux x86_64; rv:10.0.5) Gecko/20120606 Firefox/10.0.5";
        InputStream inputStream = FileInputStream.class.getResourceAsStream("/regexes.yaml");
        System.out.println(inputStream.toString());
        inputStream.close();
        Parser uaParser = new Parser();
        Client c = uaParser.parse(ua);

        System.out.println(c.userAgent.family); // =>
        System.out.println(c.userAgent.major);  // =>
        System.out.println(c.userAgent.minor);  // =>

        System.out.println(c.os.family);        // =>
        System.out.println(c.os.major);         // =>
        System.out.println(c.os.minor);         // =>

        System.out.println(c.device.family);    // =>
    }



}
