package lufax.mis.cal.parse;

import java.util.HashMap;

/**
 * Created by geyanhao801 on 7/23/15.
 */
public class RefererQueryStringMapParse {
    public static HashMap<String,String> Parse(String[] clos){
        String[] temCols16 = clos;
        HashMap<String,String> refererQueryStringMap = new HashMap<String, String>();
        for (int i = 0;i < temCols16.length;i++){
            String[] temp2Cols16 = temCols16[i].split("=",2);
            if(temp2Cols16.length > 1 ){
                String refererQueryStringKey = temp2Cols16[0];
                String refererQueryStringValue = temp2Cols16[1];
                refererQueryStringMap.put(refererQueryStringKey, refererQueryStringValue);
                System.out.println("refererQueryStringMap==" + refererQueryStringMap);
            }
        }
        return refererQueryStringMap;
    }
}
