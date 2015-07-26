package lufax.mis.cal.common.record;

import org.apache.commons.lang.StringUtils;

/**
 * Created by geyanhao801 on 7/13/15.
 */
public class BackFillOdlCalEventRecord {
    public static final String dataSource = "BF_CAL";
    public static final String delimiter = "\t";

    private Long logID = 0l;
    private String remoteAddr = "";
    private String guid = "";
    private String guidPost = "";
    private String userID = "";
    private String userIDPost = "";
    private Integer abnormalType = 0;
    private Long sessionID = 0l;
    private Integer orderInSession = 0;
    private Integer orderInCalSession = 0;
    private Integer orderInPerfSession = 0;
    private Long timestamp = 0l;
    private Integer urlType = 0;




    public Long getLogID() {
        return logID;
    }

    public void setLogID(Long logID) {
        this.logID = logID;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
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

    public Integer getAbnormalType() {
        return abnormalType;
    }

    public void setAbnormalType(Integer abnormalType) {
        this.abnormalType = abnormalType;
    }

    public Long getSessionID() {
        return sessionID;
    }

    public void setSessionID(Long sessionID) {
        this.sessionID = sessionID;
    }

    public Integer getOrderInSession() {
        return orderInSession;
    }

    public void setOrderInSession(Integer orderInSession) {
        this.orderInSession = orderInSession;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getOrderInCalSession() {
        return orderInCalSession;
    }

    public void setOrderInCalSession(Integer orderInCalSession) {
        this.orderInCalSession = orderInCalSession;
    }

    public Integer getOrderInPerfSession() {
        return orderInPerfSession;
    }

    public void setOrderInPerfSession(Integer orderInPerfSession) {
        this.orderInPerfSession = orderInPerfSession;
    }

    public Integer getUrlType() {
        return urlType;
    }

    public void setUrlType(Integer urlType) {
        this.urlType = urlType;
    }

    public Long getKey() {
        return logID;
    }

    public void setKey() {
        return;
    }

    public String getValue() {
        StringBuilder builder = new StringBuilder();

        builder.append(BackFillOdlCalEventRecord.dataSource);
        builder.append(delimiter);

        builder.append(logID);
        builder.append(delimiter);

        builder.append(remoteAddr);
        builder.append(delimiter);

        builder.append(guid);
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

        builder.append(orderInCalSession);
        builder.append(delimiter);

        builder.append(orderInPerfSession);
        builder.append(delimiter);

        builder.append(timestamp);
        builder.append(delimiter);

        builder.append(urlType);
        return builder.toString();
    }

    public void setValue(String context) {
        String[] cols = StringUtils.splitPreserveAllTokens(context, delimiter);
        /**
        if (!cols[0].equals(BackFillOdlCalEventRecord.dataSource)) {
            throw new RuntimeException("Expect " + BackFillOdlCalEventRecord.dataSource + ", but " + cols[0]);
        }**/

        logID = Long.parseLong(cols[1]);
        remoteAddr = cols[2];
        guid = cols[3];
        guidPost = cols[4];
        userID = cols[5];
        userIDPost = cols[6];
        abnormalType = Integer.parseInt(cols[7]);
        sessionID = Long.parseLong(cols[8]);
        orderInSession = Integer.parseInt(cols[9]);
        orderInCalSession = Integer.parseInt(cols[10]);
        orderInPerfSession = Integer.parseInt(cols[11]);
        timestamp = Long.parseLong(cols[12]);
        urlType = Integer.parseInt(cols[13]);
    }

    public static void main(String[] args) {
        //String context = "CAL\t000001859b0dc68d3f3e5fef0b37668f\t222.66.87.163\t192.168.86.97\thttp://list.lufax.web/list/service/users/955844/get-account-balance\tlist_get_accnt_balance\thttp://www.lufax.com/?lufax_ref=https%3A%2F%2Fmy.lufax.com%2Fmy%2Faccount\tsite_homepage\tGET\t/list\t\t\tAJAX_SYS\tWindows XP\tOther\tIE 8.0\t{"_":"1430895299156"}\t{"lufax_ref":"https://my.lufax.com/my/account"}	{"os":{"family":"Windows XP"},"string":"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET4.0C; .NET4.0E)","device":{"family":"Other"},"user_agent":{"family":"IE","major":"8","minor":"0"}}	{" Hm_lvt_9842c7dcbbff3109ea37b7407dd0e95c":"1429841626,1429852586,1430889027,1430894679"," _g2":"a3907263-36f8-403d-aa3c-267c5c21564d"," _ska":"zh-ljbtj"," _token":"\\x22YWE5YjU3MmZhZGVmN2Y2N2Q3Y2U5OWNmZGFjN2ZiNDRhMGM1NDRhNTo5NTU4NDQ6MTQzMDg5NTI5MjM0NA\u003d\u003d\\x22"," _tnf":"1"," _g":"45cf7977-e17b-4360-af42-df2e9d012ba7"," IMVC":"c79ed2ce990f4287b08cecab3390798a"," _ishbfp":"1"," __utmb":"84260612.2.10.1430894680"," _lufaxSID":"\\x227f5bd339-7c54-4ffb-89af-a1cb02d9f22b,UqDM2urSpMBhhaTsC+ScJooB9DkFgJ4EqxqtMFfN15/v1m8UpwdnoPbwpT7ag8w9PzkzZ5KI0gmYVqdo1ohx2A\u003d\u003d\\x22"," _tn":"\\x22RUQ1ODE3MUNFMzcwMkRENkRGNDJFQkZDQ0ExNjY2ODE\u003d\\x22"," __utmc":"84260612"," __utmz":"84260612.1424826835.1.1.utmcsr\u003d(direct)|utmccn\u003d(direct)|utmcmd\u003d(none)"," __utma":"84260612.2041156621.1424826835.1430889027.1430894680.13","Hm_lpvt_9842c7dcbbff3109ea37b7407dd0e95c":"1430895299"}	{}				{}	1433841855367	{}			{}")

        String context = "BF_CAL\t25971930\t222.66.87.163\t3\t4\t5\t6\t70\t0\t0\t0\t0\t1433841863641\t1";

        BackFillOdlCalEventRecord backRecord = new BackFillOdlCalEventRecord();
        backRecord.setValue(context);

        System.out.println(backRecord.getValue());


    }
}
