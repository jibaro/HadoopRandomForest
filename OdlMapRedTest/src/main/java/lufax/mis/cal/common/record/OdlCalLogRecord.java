package lufax.mis.cal.common.record;

import lufax.mis.cal.parse.QueryStringMapParse;
import lufax.mis.cal.parse.RefererQueryStringMapParse;
import lufax.mis.cal.utils.JsonUtils;
import lufax.mis.cal.utils.UserAgentUtils;
import org.apache.commons.lang.StringUtils;
import ua_parser2.Client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * Created by geyanhao801 on 7/10/15.
 */
public class OdlCalLogRecord {
    public static final String dataSource = "CAL";
    public static final String delimiter = "\t";

    //manifest configure to judge dataSource legal or not:
    public Boolean isLegal = false;

    //using name as same as odlcallog:
    private Long logID = 0L;
    private String  remoteAddr = "";
    private String  remoteHost = "";
    private Integer  remotePort = 0;
    private String  localAddr = "";
    private String  localHost = "";
    private Integer  lccalPort = 0;
    private String  url = "";
    private String  method = "";
    private String  protocol = "";
    private String  contentPath = "";
    private String  pathInfo = "";

    private HashMap<String, String> queryStringMap = new HashMap<String, String>();
    private String  queryString = "";

    private String  contentType = "";
    private Integer  contentLength = 0;
    private String  characterEncoding = "";
    private String  referer = "";
    private String  refererUrl = "";

    private HashMap<String, String> refererQueryStringMap = new HashMap<String, String>();

    private String userAgent = "";
    private Client client = null;
    private String osType = "";
    private String deviceType = "";
    private String browserType = "";



    private String cookies = "";
    private HashMap<String, String> marketCookies = new HashMap<String, String>();

    private List<HashMap<String, String>> headers = new ArrayList<HashMap<String, String>>();
    private String params = "";
    private String  requestTime = "";
    private String  actionName = "";
    private String  guid = "";
    private String  guidPost = "";
    private String  lufaxSid = "";
    private Integer  userID = 0;
    private Integer  userIDPre = 0;
    private Integer  userIDPost = 0;
    private String  createdAt = "";
    private String  updatedAt = "";
    private String  refererAction = "";
    private String  rd = "";
    private String  rawData = "";
    private Timestamp tempTimestamp = new Timestamp(0l);
    private Long timestamp = 0l;
    private String tid = "";






    public Boolean getIsLegal(){
        return isLegal;
    }

    public Long getLogID(){
        return logID;
    }
    public void setLogID(Long logID){
        this.logID = logID;
    }

    public String getRemoteAddr(){
        return remoteAddr;
    }
    public void setRemoteAddr(String remoteAddr){
        this.remoteAddr = remoteAddr;
    }

    public String getRemoteHost(){
        return remoteHost;
    }
    public void setRemoteHost(String remoteHost){
        this.remoteHost = remoteHost;
    }

    public Integer getRemotePort(){
        return remotePort;
    }
    public void setRemotePort(Integer remotePort){
        this.remotePort = remotePort;
    }

    public String getLocalAddr(){
        return localAddr;
    }
    public void setLocalAddr(String localAddr){
        this.remoteAddr = localAddr;
    }

    public String getLocalHost(){
        return localHost;
    }
    public void setLocalHost(String localHost){
        this.localHost = localHost;
    }

    public Integer getLccalPort(){
        return lccalPort;
    }
    public void setLccalPort(Integer lccalPort){
        this.lccalPort = lccalPort;
    }

    public String getUrl(){
        return url;
    }
    public void setUrl(String url){
        this.url = url;
    }

    public String getMethod(){
        return method;
    }
    public void setMethod(String method){
        this.method = method;
    }

    public String getProtocol(){
        return protocol;
    }
    public void setProtocol(String protocol){
        this.protocol = protocol;
    }

    public String getContentPath(){
        return contentPath;
    }
    public void setContentPath(String contentPath){
        this.contentPath = contentPath;
    }

    public String getPathInfo(){
        return pathInfo;
    }
    public void setPathInfo(String pathInfo){
        this.pathInfo = pathInfo;
    }

    public String getQueryString(){
        return queryString;
    }
    public void setQueryString(String queryString){
        this.queryString = queryString;
    }

    public HashMap<String, String> getQueryStringMap(){
        return queryStringMap;
    }
    public void setQueryStringMap(HashMap<String, String> queryStringMap){
        this.queryStringMap = queryStringMap;
    }

    public String getContentType(){
        return contentType;
    }
    public void setContentType(String contentType){
        this.contentType = contentType;
    }

    public Integer getContentLength(){
        return contentLength;
    }
    public void setContentLength(Integer contentLength){
        this.contentLength = contentLength;
    }

    public String getCharacterEncoding(){
        return characterEncoding;
    }
    public void setCharacterEncoding(String characterEncoding){
        this.characterEncoding = characterEncoding;
    }

    public String getReferer(){
        return referer;
    }
    public void setReferer(String referer){
        this.referer = referer;
    }

    public HashMap<String, String> getRefererQueryStringMap(){
        return refererQueryStringMap;
    }
    public void setRefererQueryStringMapr(HashMap<String, String> refererQueryStringMap){
        this.refererQueryStringMap = refererQueryStringMap;
    }

    public String getUserAgent(){
        return userAgent;
    }

    public void setUserAgent(String userAgent){
        this.userAgent = userAgent;
    }

    public String getOsType(){
        return osType;
    }

    public void setOsType(String osType){
        this.osType = osType;
    }

    public String getDeviceType(){
        return deviceType;
    }

    public void setDeviceType(String deviceType){
        this.deviceType = deviceType;
    }

    public String getBrowserType(){
        return browserType;
    }

    public void setBrowserType(String browserType){
        this.browserType = browserType;
    }


    public String getCookies(){
        return cookies;
    }
    public void setCookies(String cookies){
        this.cookies = cookies;
    }

    public HashMap<String, String> getMarketcCookies(){
        return marketCookies;
    }
    public void setMarketcCookies(HashMap<String, String> marketCookies){
        this.marketCookies = marketCookies;
    }


    public  List<HashMap<String, String>> getHeaders(){
        return headers;
    }
    public void setHeaders( List<HashMap<String, String>> headers){
        this.headers = headers;
    }

    public String getParams(){
        return params;
    }
    public void setParams(String params){
        this.params = params;
    }

    public String getRequestTime(){
        return requestTime;
    }
    public void setRequestTime(String requestTime){
        this.requestTime = requestTime;
    }

    public Long getTimestamp(){
        return timestamp;
    }
    public void setTimestamp(Long timestamp){
        this.timestamp = timestamp;
    }

    public String getActionName(){
        return actionName;
    }
    public void setActionName(String actionName){
        this.actionName = actionName;
    }

    public String getGuid(){
        return guid;
    }
    public void setGuid(String guid){
        this.guid = guid;
    }

    public String getGuidPost(){
        return guidPost;
    }
    public void setGuidPost(String guidPost){
        this.guidPost = guidPost;
    }

    public String getLufaxSid(){
        return lufaxSid;
    }
    public void setLufaxSid(String lufaxSid){
        this.lufaxSid = lufaxSid;
    }

    public Integer getUserID(){
        return userID;
    }
    public void setUserID(Integer userID){
        this.userID = userID;
    }

    public Integer getUserIDPre(){
        return userIDPre;
    }
    public void setUserIDPre(Integer userIDPre){
        this.userIDPre = userIDPre;
    }

    public Integer getUserIDPost(){
        return userIDPost;
    }
    public void setUserIDPost(Integer userIDPost){
        this.userIDPost = userIDPost;
    }

    public String getCreatedAt(){
        return createdAt;
    }
    public void setCreatedAt(String createdAt){
        this.createdAt = createdAt;
    }

    public String getUpdatedAt(){
        return updatedAt;
    }
    public void setUpdatedAt(String updatedAt){
        this.updatedAt = updatedAt;
    }

    public String getRefererAction(){
        return refererAction;
    }
    public void setRefererAction(String refererAction){
        this.refererAction = refererAction;
    }

    public String getRd(){
        return rd;
    }
    public void setRd(String rd){
        this.rd = rd;
    }

    public String getRawData(){
        return rawData;
    }
    public void setRawData(String rawData){
        this.rawData = rawData;
    }

    public String getTid(){
        return tid;
    }
    public void setTid(String tid){
        this.tid = tid;
    }







    //Get  key:
    public Long getKey(){
        return logID;
    }

    //Get and set value:
    public String getValue(){
        return toString();
    }

    public void setValue(String context){
        try {
            String[] cols = StringUtils.splitPreserveAllTokens(context, delimiter);
            System.out.println("url[7] == "+cols[7]);

            /**
            System.out.println("length == "+cols.length);
            System.out.println("logID[0] == "+cols[0]);
            System.out.println("remoteAdd == "+cols[1]);
            System.out.println("remoteHost[2] == "+cols[2]);
            System.out.println("remotePort[3] == "+cols[3]);
            System.out.println("localAddr[4] == "+cols[4]);
            System.out.println("localHost[5] == "+cols[5]);
            System.out.println("lccalPort[6] == "+cols[6]);
            System.out.println("method[8] == "+cols[8]);
            System.out.println("protocol[9] == "+cols[9]);
            System.out.println("contentPath[10] == "+cols[10]);
            System.out.println("pathInfo[11] == "+cols[11]);
            System.out.println("queryString[12] == "+cols[12]);
            System.out.println("contentType[13] == "+cols[13]);
            System.out.println("contentLength[14] == "+cols[14]);
            System.out.println("characterEncoding[15] == "+cols[15]);
            System.out.println("referer[16] == "+cols[16]);
            System.out.println("userAgent[17] == "+cols[17]);
            System.out.println("cookies[18] == "+cols[18]);
            System.out.println("headers[19] == "+cols[19]);
            System.out.println("params[20] == "+cols[20]);
            System.out.println("requestTime[21] == "+cols[21]);
            System.out.println("actionName[22] == "+cols[22]);
            System.out.println("guid[23] == "+cols[23]);
            System.out.println("guidPost[24] == "+cols[24]);
            System.out.println("lufaxSid[25] == "+cols[25]);
            System.out.println("userID[26] == "+cols[26]);
            System.out.println("userIDPre[27] == "+cols[27]);
            System.out.println("userIDPost[28] == "+cols[28]);
            System.out.println("createdAt[29] == "+cols[29]);
            System.out.println("updatedAt[30] == "+cols[30]);
            System.out.println("refererAction[31] == "+cols[31]);
            System.out.println("rd[32] == "+cols[32]);
            **/
             /**
            if (!cols[0].equals(OdlCalLogRecord.dataSource)) {
                throw new RuntimeException("Expect " + OdlCalLogRecord.dataSource + ", but " + cols[0]);
            }**/



            logID = Long.parseLong(cols[0]);
            remoteAddr = cols[1];
            remoteHost = cols[2];
            remotePort = Integer.parseInt(cols[3]);
            localAddr = cols[4];
            localHost = cols[5];
            lccalPort = Integer.parseInt(cols[6]);
            url = cols[7];
            method = cols[8];
            protocol = cols[9];
            contentPath = cols[10];
            pathInfo = cols[11];

            queryString = QueryStringMapParse.parse(cols[12]);
            System.out.println("queryStringMap ==" + queryString);

            contentType = cols[13];
            contentLength = Integer.parseInt(cols[14]);
            characterEncoding = cols[15];
            referer = cols[16];
            String[] tempCols16 = cols[16].split("\\?");

            //parse refererUrl
            refererUrl = tempCols16[0];
            System.out.println("refererUrl==" + refererUrl);

            //parse refererQueryStringMap
            if (tempCols16.length > 1){
                refererQueryStringMap = RefererQueryStringMapParse.Parse(tempCols16);
            }

            userAgent = cols[17];
            client = UserAgentUtils.uaParse(userAgent);
            osType = client.os.family;
            deviceType = client.device.family;
            browserType = client.userAgent.family;

            cookies = JsonUtils.parseJsonstrToString(cols[18]);
            System.out.println("cookies==" + cookies);


            //parse TID from cookies;
            for ( HashMap<String,String> tempCookies :JsonUtils.parseListJson2Map(cols[18])){
                if (tempCookies.containsValue("marketCookie")){
                    marketCookies = tempCookies;
                    System.out.println("marketCookies==" + marketCookies);
                    String[] tempValue = StringUtils.splitPreserveAllTokens(marketCookies.get("value"), ">");
                    tid = tempValue[1];
                    System.out.println("tid==" + tid);

                }
            }

            headers = JsonUtils.parseListJson2Map(cols[19]);
            params = JsonUtils.parseJsonstrToString(cols[20]);
            requestTime = cols[21];
            tempTimestamp =Timestamp.valueOf(requestTime);
            timestamp = tempTimestamp.getTime();
            actionName = cols[22];
            guid = cols[23];
            guidPost = cols[24];
            lufaxSid = cols[25];

            if (cols[26].isEmpty() || cols[26].equals("NULL")){
                userID = -1;
            }
            else {
                userID = Integer.parseInt(cols[26]);
            }

            if (cols[27].isEmpty() || cols[27].equals("NULL")){
                userIDPre = -1;
            }
            else {
                userIDPre = Integer.parseInt(cols[27]);
            }

            if (cols[28].isEmpty() || cols[28].equals("NULL")){
                userIDPost = -1;
            }
            else {
                userIDPost = Integer.parseInt(cols[28]);
            }

            createdAt = cols[29];
            updatedAt = cols[30];
            refererAction = cols[31];
            rd = cols[32];
            isLegal = true;

            System.out.println("logID[0] == "+logID);
            System.out.println("requestTime[21] == "+cols[21]);
            System.out.println("timstamp[22] == "+timestamp);
            System.out.println("marketCookies == "+marketCookies);
            System.out.println("refererQueryStringMap == "+refererQueryStringMap);
            System.out.println("osType == "+osType);
            System.out.println("deviceType == "+deviceType);
            System.out.println("browserType == "+browserType);

        } catch (Exception e) {
            e.printStackTrace();
            isLegal = false;
            rawData = context;

        }
    }

    //toString
    public String toString(){
        StringBuilder builder = new StringBuilder();

        builder.append(OdlCalLogRecord.dataSource);
        builder.append(delimiter);

        if(!isLegal){
            builder.append(rawData);
            return builder.toString();
        }

        builder.append(logID);
        builder.append(delimiter);

        builder.append(remoteAddr);
        builder.append(delimiter);

        builder.append(remoteHost);
        builder.append(delimiter);

        builder.append(remotePort);
        builder.append(delimiter);

        builder.append(localAddr);
        builder.append(delimiter);

        builder.append(localHost);
        builder.append(delimiter);

        builder.append(lccalPort);
        builder.append(delimiter);

        builder.append(url);
        builder.append(delimiter);

        builder.append(method);
        builder.append(delimiter);

        builder.append(protocol);
        builder.append(delimiter);

        builder.append(contentPath);
        builder.append(delimiter);

        builder.append(pathInfo);
        builder.append(delimiter);

        builder.append(queryString);
        builder.append(delimiter);

        builder.append(contentType);
        builder.append(delimiter);

        builder.append(contentLength);
        builder.append(delimiter);

        builder.append(characterEncoding);
        builder.append(delimiter);

        builder.append(referer);
        builder.append(delimiter);

        builder.append(userAgent);
        builder.append(delimiter);

        builder.append(cookies);
        builder.append(delimiter);

        builder.append(headers);
        builder.append(delimiter);

        builder.append(params);
        builder.append(delimiter);

        builder.append(requestTime);
        builder.append(delimiter);

        builder.append(actionName);
        builder.append(delimiter);

        builder.append(guid);
        builder.append(delimiter);

        builder.append(guidPost);
        builder.append(delimiter);

        builder.append(lufaxSid);
        builder.append(delimiter);

        builder.append(userID);
        builder.append(delimiter);

        builder.append(userIDPre);
        builder.append(delimiter);

        builder.append(userIDPost);
        builder.append(delimiter);

        builder.append(createdAt);
        builder.append(delimiter);

        builder.append(updatedAt);
        builder.append(delimiter);

        builder.append(refererAction);
        builder.append(delimiter);

        builder.append(rd);
        builder.append(delimiter);

        return builder.toString();

    }

    public static void main(String[] args){

        File file = new File("/home/geyanhao801/Desktop/cal_log.txt");
        BufferedReader reader = null;

        String tempreader = null;
        try {
            System.out.println("Read one line of the file for one iteration");
            reader = new BufferedReader(new FileReader(file));

            int line = 0;

            //For BufferdReader, read one line for each time and save it into 'tempreader';
            while ((tempreader = reader.readLine()) != null) {

                System.out.println("No. line" + line);
                line++;

                //Put data of tempreader into record for further MR processing
                OdlCalLogRecord record = new OdlCalLogRecord();
                record.setValue(tempreader);

                //Check the value parse
                if (record.getIsLegal()) {
                    System.out.println("line " + line + ": " + record.getValue());
                }
            }
            reader.close();
        }
        catch(IOException e){
            e.printStackTrace();
        }
        catch (Exception e){
            System.out.println(tempreader);
        }finally {
            if (reader != null){
                try{
                    reader.close();
                }catch (IOException el){

                }
            }

        }

    }

}
