package lufax.mis.cal.common.record;

import org.apache.commons.lang.StringUtils;

/**
 * Created by lufax on 6/29/15.
 */
public class TSessionRecord {
    public static final String dataSource = "T_SESS";
//    public static final String delimiter = "\u0001";
    public static final String delimiter = "\t";

    private String id = "";
    private String sessionID = "";
    private String userID = "";
    private String partyNo = "";
    private String startTime = "";
    private String accessTime = "";
    private String endTime = "";
    private String loginType = "";
    private String loginConsole = "";
    private String loginIP = "";
    private String randomNum = "";
    private String isSecure = "";
    private String isValid = "";
    private String invalidReason = "";
    private String createdAt = "";
    private String updatedAt = "";
    private String enterpriseFlag = "";
    private String uniqueToken = "";

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUserID() {
        return userID;
    }

    public String getSessionID() {
        return sessionID;
    }

    public void setSessionID(String sessionID) {
        this.sessionID = sessionID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getPartyNo() {
        return partyNo;
    }

    public void setPartyNo(String partyNo) {
        this.partyNo = partyNo;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getAccessTime() {
        return accessTime;
    }

    public void setAccessTime(String accessTime) {
        this.accessTime = accessTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getLoginType() {
        return loginType;
    }

    public void setLoginType(String loginType) {
        this.loginType = loginType;
    }

    public String getLoginConsole() {
        return loginConsole;
    }

    public void setLoginConsole(String loginConsole) {
        this.loginConsole = loginConsole;
    }

    public String getLoginIP() {
        return loginIP;
    }

    public void setLoginIP(String loginIP) {
        this.loginIP = loginIP;
    }

    public String getRandomNum() {
        return randomNum;
    }

    public void setRandomNum(String randomNum) {
        this.randomNum = randomNum;
    }

    public String getIsSecure() {
        return isSecure;
    }

    public void setIsSecure(String isSecure) {
        this.isSecure = isSecure;
    }

    public String getIsValid() {
        return isValid;
    }

    public void setIsValid(String isValid) {
        this.isValid = isValid;
    }

    public String getInvalidReason() {
        return invalidReason;
    }

    public void setInvalidReason(String invalidReason) {
        this.invalidReason = invalidReason;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getEnterpriseFlag() {
        return enterpriseFlag;
    }

    public void setEnterpriseFlag(String enterpriseFlag) {
        this.enterpriseFlag = enterpriseFlag;
    }

    public String getUniqueToken() {
        return uniqueToken;
    }

    public void setUniqueToken(String uniqueToken) {
        this.uniqueToken = uniqueToken;
    }

    private String getKey() {
        return id;
    }

    private void setKey() {
        return;
    }

    private String getValue() {
        StringBuilder builder = new StringBuilder();

        builder.append(TSessionRecord.dataSource);
        builder.append(delimiter);

        builder.append(id);
        builder.append(delimiter);

        builder.append(sessionID);
        builder.append(delimiter);

        builder.append(userID);
        builder.append(delimiter);

        builder.append(partyNo);
        builder.append(delimiter);

        builder.append(startTime);
        builder.append(delimiter);

        builder.append(accessTime);
        builder.append(delimiter);

        builder.append(endTime);
        builder.append(delimiter);

        builder.append(loginType);
        builder.append(delimiter);

        builder.append(loginConsole);
        builder.append(delimiter);

        builder.append(loginIP);
        builder.append(delimiter);

        builder.append(randomNum);
        builder.append(delimiter);

        builder.append(isSecure);
        builder.append(delimiter);

        builder.append(isValid);
        builder.append(delimiter);

        builder.append(invalidReason);
        builder.append(delimiter);

        builder.append(createdAt);
        builder.append(delimiter);

        builder.append(updatedAt);
        builder.append(delimiter);

        builder.append(enterpriseFlag);
        builder.append(delimiter);

        builder.append(uniqueToken);

        return builder.toString();
    }

    public void setValue(String context) {
        String[] cols = StringUtils.splitPreserveAllTokens(context, delimiter);

        if (!cols[0].equals(OdlCalLogRecord.dataSource)) {
            throw new RuntimeException("Expect " + TSessionRecord.dataSource + ", but " + cols[0]);
        }

        id = cols[1];
        sessionID = cols[2];
        userID = cols[3];
        partyNo = cols[4];
        startTime = cols[5];
        accessTime = cols[6];
        endTime = cols[7];
        loginType = cols[8];
        loginConsole = cols[9];
        loginIP = cols[10];
        randomNum = cols[11];
        isSecure = cols[12];
        isValid = cols[13];
        invalidReason = cols[14];
        createdAt = cols[15];
        updatedAt = cols[16];
        enterpriseFlag = cols[17];
        uniqueToken = cols[18];
    }

    public void setRawValue(String context) {
        String[] cols = StringUtils.splitPreserveAllTokens(context, delimiter);

        id = cols[0];
        sessionID = cols[1];
        userID = cols[2];
        partyNo = cols[3];
        startTime = cols[4];
        accessTime = cols[5];
        endTime = cols[6];
        loginType = cols[7];
        loginConsole = cols[8];
        loginIP = cols[9];
        randomNum = cols[10];
        isSecure = cols[11];
        isValid = cols[12];
        invalidReason = cols[13];
        createdAt = cols[14];
        updatedAt = cols[15];
        enterpriseFlag = cols[16];
        uniqueToken = cols[17];
    }

    public static void main(String[] args) {
        String context = "121947198\t694a1ef8-5b01-40b8-a981-2c8072dd13ae\t2040454\t849801404091294677\t2015-07-06 00:09:08\t2015-07-06 00:10:58\tNULL\t1\t2\t183.206.183.221, 192.168.86.56\t-875324749541677158\t0\t1\tNULL\t2015-07-06 00:09:08\t2015-07-06 00:10:58\t0\t2040454_2";

        TSessionRecord record = new TSessionRecord();
        record.setRawValue(context);

        System.out.println(context);
        System.out.println(record.getValue());
    }
}
