package lufax.mis.cal.parse;

import lufax.mis.cal.utils.JsonUtils;

import java.util.LinkedHashMap;

/**
 * Created by geyanhao801 on 7/22/15.
 */
public class QueryStringMapParse {
    public QueryStringMapParse(){
        return;
    }
    public static String  parse(String input){
        //parse cols[12]  namely queryString
        LinkedHashMap<String,String> ret1 = new LinkedHashMap<String, String>();
        String[] tempCols12 = input.replace("&amp;", "&").split("&");
        for (int i = 0;i < tempCols12.length;i++){
            String[] temp2Cols12 = tempCols12[i].split("=",2);
            String queryStringKey = temp2Cols12[0];
            String queryStringValue= temp2Cols12[1];
            ret1.put(queryStringKey,queryStringValue);
        }
        String queryString = JsonUtils.convertMap2Json(ret1);
        System.out.println("queryString ==" + queryString);
        System.out.println("queryStringMap ==" + queryString);
        return queryString;
    }
}
