package lufax.mis.cal.common.record;

import lufax.mis.cal.utils.JsonUtils;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lufax on 7/3/15.
 */
public class CalEventRecord {
    public static final String dataSource = "CAL_EVENT";
    public static final String delimiter = "\t";

    private String logID = "";
    private String remoteAddr = "";
    private String localAddr = "";
    private String requestTime = "";
    private String url = "";
    private String actionName = "";
    private String refererUrl = "";
    private String refererAction = "";
    private String method = "";
    private String contextPath = "";
    private String pathInfo = "";
    private String contentType = "";
    private String actionType = "";
    private String deviceType = "";
    private String osType = "";
    private String browserType = "";

    private Map<String, String> queryStringMap = new HashMap<String, String>();
    private String queryString = "";

    private Map<String, String> refererQueryStringMap = new HashMap<String, String>();
    private Map<String, String> userAgentParseMap = new HashMap<String, String>();
    private String cookieParseMap = "";
    private String paramsParseMap = "";

    private String guid = "";
    private String guidPost = "";
    private String lufaxSid = "";
    private String userID = "";
    private String userIDPost = "";
    private Map<String, String> marketCookie = new HashMap<String, String>();
    private int abnormalType = 0;
    private String sessionID = "";
    private Integer orderInSession = 0;
    private String platfromType = "";
    private String rd = "";




    public String getLogID() {
        return logID;
    }

    public void setLogID(String logID) {
        this.logID = logID;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public String getLocalAddr() {
        return localAddr;
    }

    public void setLocalAddr(String localAddr) {
        this.localAddr = localAddr;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getRequestTime() {
        return requestTime;
    }

    public void setRequestTime(String requestTime) {
        this.requestTime = requestTime;
    }

    public String getActionName() {
        return actionName;
    }

    public void setActionName(String actionName) {
        this.actionName = actionName;
    }

    public String getRefererUrl() {
        return refererUrl;
    }

    public void setRefererUrl(String refererUrl) {
        this.refererUrl = refererUrl;
    }

    public String getRefererAction() {
        return refererAction;
    }

    public void setRefererAction(String refererAction) {
        this.refererAction = refererAction;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getContextPath() {
        return contextPath;
    }

    public void setContextPath(String contextPath) {
        this.contextPath = contextPath;
    }

    public String getPathInfo() {
        return pathInfo;
    }

    public void setPathInfo(String pathInfo) {
        this.pathInfo = pathInfo;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getActionType() {
        return actionType;
    }

    public void setActionType(String actionType) {
        this.actionType = actionType;
    }

    public String getOsType() {
        return osType;
    }

    public void setOsType(String osType) {
        this.osType = osType;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getBrowserType() {
        return browserType;
    }

    public void setBrowserType(String browserType) {
        this.browserType = browserType;
    }

    public Map<String, String> getQueryStringMap() {
        return queryStringMap;
    }

    public void setQueryStringMap(Map<String, String> queryStringMap) {
        this.queryStringMap = queryStringMap;
    }

    public Map<String, String> getRefererQueryStringMap() {
        return refererQueryStringMap;
    }

    public void setRefererQueryStringMap(Map<String, String> refererQueryStringMap) {
        this.refererQueryStringMap = refererQueryStringMap;
    }

    public Map<String, String> getUserAgentParseMap() {
        return userAgentParseMap;
    }

    public void setUserAgentParseMap(Map<String, String> userAgentParseMap) {
        this.userAgentParseMap = userAgentParseMap;
    }

    public String getCookieParseMap() {
        return cookieParseMap;
    }

    public void setCookieParseMap(String cookieParseMap) {
        this.cookieParseMap = cookieParseMap;
    }

    public String getParamsParseMap() {
        return paramsParseMap;
    }

    public void setParamsParseMap(String paramsParseMap) {
        this.paramsParseMap = paramsParseMap;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getLufaxSid() {
        return lufaxSid;
    }

    public void setLufaxSid(String lufaxSid) {
        this.lufaxSid = lufaxSid;
    }



    public Map<String, String> getMarketCookie() {
        return marketCookie;
    }

    public void setMarketCookie(Map<String, String> marketCookie) {
        this.marketCookie = marketCookie;
    }



    public String getGuidPost() {
        return guidPost;
    }

    public void setGuidPost(String guidPost) {
        this.guidPost = guidPost;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getUserIDPost() {
        return userIDPost;
    }

    public void setUserIDPost(String userIDPost) {
        this.userIDPost = userIDPost;
    }

    public int getAbnormalType() {
        return abnormalType;
    }

    public void setAbnormalType(int abnormalType) {
        this.abnormalType = abnormalType;
    }

    public String getSessionID() {
        return sessionID;
    }

    public void setSessionID(String sessionID) {
        this.sessionID = sessionID;
    }

    public Integer getOrderInSession() {
        return orderInSession;
    }

    public void setOrderInSession(Integer orderInSession) {
        this.orderInSession = orderInSession;
    }

    public String getPlatfromType() {
        return platfromType;
    }

    public void setPlatfromType(String platfromType) {
        this.platfromType = platfromType;
    }

    public String getRd() {
        return rd;
    }

    public void setRd(String rd) {
        this.rd = rd;
    }

    public String getKey() {
        return logID;
    }

    public void setKey() {
        return;
    }

    public String getValue() {
        StringBuilder builder = new StringBuilder();

        builder.append(CalEventRecord.dataSource);
        builder.append(delimiter);

        builder.append(logID);
        builder.append(delimiter);

        builder.append(remoteAddr);
        builder.append(delimiter);

        builder.append(localAddr);
        builder.append(delimiter);

        builder.append(url);
        builder.append(delimiter);

        builder.append(actionName);
        builder.append(delimiter);

        builder.append(refererUrl);
        builder.append(delimiter);

        builder.append(refererAction);
        builder.append(delimiter);

        builder.append(method);
        builder.append(delimiter);

        builder.append(contextPath);
        builder.append(delimiter);

        builder.append(pathInfo);
        builder.append(delimiter);

        builder.append(contentType);
        builder.append(delimiter);

        builder.append(actionType);
        builder.append(delimiter);

        builder.append(osType);
        builder.append(delimiter);

        builder.append(deviceType);
        builder.append(delimiter);

        builder.append(browserType);
        builder.append(delimiter);

        builder.append(queryString);
        builder.append(delimiter);

        builder.append(JsonUtils.convertMap2Json(refererQueryStringMap));
        builder.append(delimiter);

        builder.append(JsonUtils.convertMap2Json(userAgentParseMap));
        builder.append(delimiter);

        builder.append(cookieParseMap);
        builder.append(delimiter);

        builder.append(paramsParseMap);
        builder.append(delimiter);

        builder.append(guid);
        builder.append(delimiter);

        builder.append(lufaxSid);
        builder.append(delimiter);

        builder.append(marketCookie.toString());
        builder.append(delimiter);

        builder.append(guidPost);
        builder.append(delimiter);

        builder.append(userID);
        builder.append(delimiter);

        builder.append(userIDPost);
        builder.append(delimiter);

        builder.append(abnormalType);
        builder.append(delimiter);

        builder.append(sessionID);
        builder.append(delimiter);

        builder.append(orderInSession);
        builder.append(delimiter);

        return builder.toString();
    }

    public void setValue(String context) {
        String[] cols = StringUtils.splitPreserveAllTokens(context, delimiter);

        if (!cols[0].equals(OdlCalLogRecord.dataSource)) {
            throw new RuntimeException("Expect " + OdlCalLogRecord.dataSource + ", but " + cols[0]);
        }

        logID = cols[1];
        remoteAddr = cols[2];
        localAddr = cols[3];
        url = cols[4];
        actionName = cols[5];
        refererUrl = cols[6];
        refererAction = cols[7];
        method = cols[8];
        contextPath = cols[9];
        pathInfo = cols[10];
        contentType = cols[11];
        actionType = cols[12];
        osType = cols[13];
        deviceType = cols[14];
        browserType = cols[15];
        queryString = cols[16];
        refererQueryStringMap = JsonUtils.parseJson2MapString(cols[17]);
        userAgentParseMap = JsonUtils.parseJson2MapString(cols[18]);
        cookieParseMap = cols[19];
        paramsParseMap = cols[20];
        guid = cols[21];
        lufaxSid = cols[22];
        marketCookie= JsonUtils.parseJson2MapString(cols[24]);

        guidPost = cols[30];
        userID = cols[31];
        userIDPost = cols[32];
        abnormalType = Integer.parseInt(cols[33]);
        sessionID = cols[34];
        orderInSession = Integer.parseInt(cols[35]);

    }

    public void setValue(OdlCalLogRecord calLog, BackFillOdlCalEventRecord backFillCalEvent) {
        logID = Long.toString(calLog.getLogID());
        remoteAddr = calLog.getRemoteAddr();
        localAddr = calLog.getLocalAddr();
        url = calLog.getUrl();
        actionName = calLog.getActionName();
        refererUrl = calLog.getUrl();
        refererAction = calLog.getRefererAction();
        method = calLog.getMethod();
        contextPath = calLog.getContentPath();
        pathInfo = calLog.getPathInfo();
        contentType = calLog.getContentType();
        actionType = calLog.getActionName();
        osType = calLog.getOsType();
        deviceType = calLog.getDeviceType();
        browserType = calLog.getBrowserType();
        queryString = calLog.getQueryString();
        refererQueryStringMap = calLog.getRefererQueryStringMap();
        //userAgentParseMap = calLog.getUserAgent();
        cookieParseMap = calLog.getCookies();
        paramsParseMap = calLog.getParams();
        guid = calLog.getGuid();
        guidPost = backFillCalEvent.getGuidPost();
        lufaxSid = calLog.getLufaxSid();
        userID = backFillCalEvent.getUserID();
        userIDPost = backFillCalEvent.getUserIDPost();
        marketCookie = calLog.getMarketcCookies();
        abnormalType = backFillCalEvent.getAbnormalType();
        sessionID = Long.toString(backFillCalEvent.getSessionID());
        orderInSession = backFillCalEvent.getOrderInSession();
        //platfromType = calLog.get();
        rd = calLog.getRd();


    }
}
