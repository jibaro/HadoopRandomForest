package lufax.mis.cal.odlmr.action;

import lufax.mis.cal.common.MisProperties;
import lufax.mis.cal.odlmr.mr.FillCalEventUseridMR;
import lufax.mis.cal.utils.HdfsUtils;
import lufax.mis.cal.utils.LogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.util.StringUtils;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by geyanhao801 on 7/21/15.
 */
public class FillCalEventUserIdAction {
    private String intermediateCalEventUserIDPath = "hdfs://ha-service1/user/mis/intermediate/fill_cal_event_userid/";
    private String intermediateCalEventGuidPath = "hdfs://ha-service1/user/mis/intermediate/fill_cal_event_guid/";

    private String configPath = "";

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public String getIntermediateCalEventUserIDPath() {
        return intermediateCalEventUserIDPath;
    }

    public void setIntermediateCalEventUserIDPath(String intermediateCalEventUserIDPath) {
        intermediateCalEventUserIDPath = intermediateCalEventUserIDPath;
    }

    public String getIntermediateCalEventGuidPath() {
        return intermediateCalEventGuidPath;
    }

    public void setIntermediateCalEventGuidPath(String intermediateCalEventGuidPath) {
        intermediateCalEventGuidPath = intermediateCalEventGuidPath;
    }

    public String getInputFilePath(String partitionDate) {
        return intermediateCalEventGuidPath + partitionDate;
    }

    public String getOutputFilePath(String partitionDate) {
        return intermediateCalEventUserIDPath + partitionDate;
    }

    public int fillCalEventUserId(String partitionDate) throws URISyntaxException {
        List<String> inputs = new ArrayList<String>();

        inputs.add(getInputFilePath(partitionDate));

        String outputs = getOutputFilePath(partitionDate);

        boolean ret = HdfsUtils.deleteOutputFilePath(outputs);
        if (!ret) {
            //说明删除目标目录不成功
//			System.out.print(new Date());
            LogUtils.log2Screen("Delete target diretory faild, don't start mapreduce");
            return -1;
        }
        String[] args = new String[3];
        args[0] = StringUtils.collectionToDelimitedString(inputs, ",");
        args[1] = outputs;
        args[2] = partitionDate;

        Configuration conf = new Configuration();

        MisProperties properties = MisProperties.getMisProperties(configPath);
        for (Map.Entry<Object, Object> entry : properties.getProps().entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            conf.set(key, value);
        }

        conf.set("fill.cal.event.userid.target.partition", partitionDate);

        int res = -1;
        try {
            res = ToolRunner.run(conf, new FillCalEventUseridMR(), args);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return res;


    }

}
