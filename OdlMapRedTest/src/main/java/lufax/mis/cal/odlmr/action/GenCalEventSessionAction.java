package lufax.mis.cal.odlmr.action;

import lufax.mis.cal.common.MisProperties;
import lufax.mis.cal.odlmr.mr.GenCalEventSessionMR;
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
 * Created by geyanhao801 on 7/17/15.
 */
public class GenCalEventSessionAction {
    private String IntermediateCalEventUserIDPath = "hdfs://ha-service1/user/mis/intermediate/fill_cal_event_userid/";
    private String IntermediateCalEventSessionPath = "hdfs://ha-service1/user/mis/intermediate/cal_event_session/";

    private String configPath = "";

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public String getIntermediateCalEventUserIDPath() {
        return IntermediateCalEventUserIDPath;
    }

    public void setIntermediateCalEventUserIDPath(String intermediateCalEventUserIDPath) {
        IntermediateCalEventUserIDPath = intermediateCalEventUserIDPath;
    }

    public String getIntermediateCalEventSessionPath() {
        return IntermediateCalEventSessionPath;
    }

    public void setIntermediateCalEventSessionPath(String intermediateCalEventSessionPath) {
        IntermediateCalEventSessionPath = intermediateCalEventSessionPath;
    }

    public String getInputFilePath(String partitionDate) {
        return IntermediateCalEventUserIDPath + partitionDate;
    }

    public String getOutputFilePath(String partitionDate) {
        return IntermediateCalEventSessionPath + partitionDate;
    }

    public int genCalEventSession(String partitionDate) throws URISyntaxException {
        List<String> inputs = new ArrayList<String>();
        inputs.add(getInputFilePath(partitionDate));

        String output = getOutputFilePath(partitionDate);

        boolean ret = HdfsUtils.deleteOutputFilePath(output);

        if (!ret) {
            //说明删除目标目录不成功
//			System.out.print(new Date());
            LogUtils.log2Screen("Delete target diretory faild, don't start mapreduce");
            return -1;
        }

        String[] args = new String[3];
        args[0] = StringUtils.collectionToDelimitedString(inputs,",");
        args[1] = output;
        args[2] = partitionDate;

        Configuration conf = new Configuration();

        MisProperties properties = MisProperties.getMisProperties(configPath);
        for (Map.Entry<Object, Object> entry : properties.getProps().entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            conf.set(key, value);
        }

        conf.set("gen.cal.event.session.target.partition", partitionDate);

        int res = -1;
        try {
            res = ToolRunner.run(conf, new GenCalEventSessionMR(), args);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return res;
    }
}
