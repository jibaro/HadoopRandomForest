package lufax.mis.cal.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lufax on 7/1/15.
 */

public class JsonUtils {
    public static List<HashMap<String, String>> parseListJson2Map(String jsonStr) {
        Gson gson = new Gson();

        //Map<String, Object> nestMap = gson.fromJson(jsonStr, Map.class);

        List<HashMap<String, String>> nestMap = gson.fromJson(jsonStr, new TypeToken<List<HashMap>>(){}.getType());

        return nestMap;
    }


    public static HashMap<String, Object> parseJson2Map(String jsonStr) {
        Gson gson = new Gson();

        HashMap<String, Object> nestMap = gson.fromJson(jsonStr, HashMap.class);

        return nestMap;
    }

    public static HashMap<String, String > parseJson2MapString(String jsonStr) {
        Gson gson = new Gson();

        HashMap<String, String> nestMap = gson.fromJson(jsonStr, HashMap.class);

        return nestMap;
    }

    public static String convertListMap2Json(List<HashMap<String, String>> params) {
        Gson gson = new Gson();
        return gson.toJson(params);


    }
    public static String convertListObjectMap2Json(List<HashMap<String, Object>> params) {
        Gson gson = new Gson();
        return gson.toJson(params);


    }
    public static String convertMap2Json(Map<String, String> params) {
        Gson gson = new Gson();
        return gson.toJson(params);
    }
//
//    public static String convertNestMap2Json(Map<String, Map<String, String>> params) {
//        Gson gson = new Gson();
//
//        Type type = new TypeToken<Map<String, Map<String, String>>>(){}.getType();
//        String json = gson.toJson(params, type);
//        return json;
//    }
    public static String parseJsonstrToString(String jsonstr) {
        LinkedHashMap<String,String> ret1 = new LinkedHashMap<String, String>();
        List<HashMap<String, String>> ret2 = JsonUtils.parseListJson2Map(jsonstr);

        for (HashMap<String,String> ret3 :ret2){
            ret1.put(ret3.get("name"), ret3.get("value"));
        }
        String jsonParse = JsonUtils.convertMap2Json(ret1);
        return jsonParse;
    }

    public static void main(String[] args) {
        //String jsonStr = "{\"string\":\"abc\",\"user_agent\":{\"family\":\"Chrome Mobile\",\"major\":\"33\",\"minor\":\"0\",\"patch\":\"0\"},\"os\":{\"patch_minor\":\"\",\"patch\":\"4\",\"family\":\"Android\",\"major\":\"4\",\"minor\":\"4\"},\"device\":{\"family\":\"M351\"}}";
        //String jsonStr = "[{\"name\":\"_g\",\"value\":\"14e34c97-03c9-4e41-ad51-bcd0bf927330\",\"maxAge\":-1,\"version\":\"0\",\"secure\":\"0\"},{\"name\":\"_g2\",\"value\":\"14e34c97-03c9-4e41-ad51-bcd0bf927330\",\"maxAge\":-1,\"version\":\"0\",\"secure\":\"0\"}]";
        //HashMap<String, Object> ret1 = JsonUtils.parseJson2Map(jsonStr);
       // String jsonOut = parseJsonstrToString(jsonStr);
        //System.out.println(jsonOut);



    }
}
