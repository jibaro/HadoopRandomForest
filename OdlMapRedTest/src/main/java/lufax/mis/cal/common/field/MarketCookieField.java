package lufax.mis.cal.common.field;

import lufax.mis.cal.utils.JsonUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MarketCookieField {
	private Boolean isLegal = false;
	private String aid = "";
	private String tid = "";
	private String personID = "";
	private String source0 = "";
	private String source1 = "";
	private String source2 = "";
	private String optionType = "";
	private String companyID = "";
	private String exParameter = "";

    public MarketCookieField() {
    }

	public MarketCookieField(String context) {
		setValue(context);
	}

	public String getKey() {
		return "";
	}

	public void setKey(String key) {

	}

	public String getValue() {
//		JSONObject jsonObj = new JSONObject();
        Map<String, String> params = new HashMap<String, String>();

		if (!aid.trim().isEmpty()) {
            params.put("aid", aid);
		}

		if (!tid.trim().isEmpty()) {
            params.put("tid", tid);
		}

		if (!personID.trim().isEmpty()) {
            params.put("personid", personID);
		}

		if (!source0.trim().isEmpty()) {
            params.put("source_0", source0);
		}

		if (!source1.trim().isEmpty()) {
            params.put("source_1", source1);
		}

		if (!source2.trim().isEmpty()) {
            params.put("source_2", source2);
		}

		if (!optionType.trim().isEmpty()) {
            params.put("optiontype", optionType);
		}

		if (!companyID.trim().isEmpty()) {
            params.put("companyid", companyID);
		}

		if (!exParameter.trim().isEmpty()) {
            params.put("exparameter", exParameter);
		}

		return JsonUtils.convertMap2Json(params);
	}

	public void setValue(String context) {
        List<HashMap<String, String>> paramsList = JsonUtils.parseListJson2Map(context);
        for (HashMap<String,String> params :paramsList)
        {
            System.out.println(params);

            if (params.containsKey("aid")) {
                aid = params.get("aid");
            }

            if (params.containsKey("marketCookie")) {
                System.out.println(params.get("marketCookie"));

                tid = params.get("marketCookie");
            }

            if (params.containsKey("personid")) {
                personID = params.get("personid");
            }

            if (params.containsKey("source_0")) {
                source0 = params.get("source_0");
            }

            if (params.containsKey("source_1")) {
                source1 = params.get("source_1");
            }

            if (params.containsKey("source_2")) {
                source2 = params.get("source_2");
            }

            if (params.containsKey("optiontype")) {
                optionType = params.get("optiontype");
            }

            if (params.containsKey("companyid")) {
                companyID = params.get("companyid");
            }

            if (params.containsKey("exparameter")) {
                exParameter = params.get("exparameter");
            }

        }

		isLegal = true;
	}

    @Override
    public String toString() {
        return getValue();
    }


    public static void main(String[] args){
        MarketCookieField marketCookieField = new MarketCookieField();
        String str = "[{\"name\":\"marketCookie2\",\"value\":\"-1\\u003e1752096\"},{\"name\":\"marketCookie\",\"value\":\"-1\\u003e1752096\\u003e1752096\u003eC010:-1:5314\\u003e-2300\\\u003e-1\\003eeyJ1ayI6IiIsInRpZCI6IjE3NTIwOTYifQ\",\"maxAge\":-1,\"version\":\"0\",\"secure\":\"0\"}]";
        marketCookieField.setValue(str);
        System.out.println(marketCookieField.toString());
    }
}
