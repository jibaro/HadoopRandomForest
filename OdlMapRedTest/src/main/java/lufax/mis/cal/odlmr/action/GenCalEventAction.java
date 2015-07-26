package lufax.mis.cal.odlmr.action;

import lufax.mis.cal.common.MisProperties;
import lufax.mis.cal.odlmr.mr.GenCalEventMR;
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
public class GenCalEventAction {
    private String calEventPath = "hdfs://ha-service1/user/mis/hdw/odl_cal_event/";
    private String IntermediateCalEventSessionPath = "hdfs://ha-service1/user/mis/intermediate/cal_event_session/";
    private String odlCalPath = "hdfs://ha-service1/user/mis/etl/cal_log/";

    private String configPath = "";

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public String getCalEventPath() {
        return calEventPath;
    }

    public void setCalEventPath(String calEventPath) {
        this.calEventPath = calEventPath;
    }

    public String getOdlCalPath() {
        return odlCalPath;
    }

    public void setOdlCalPath(String odlCalPath) {
        this.odlCalPath = odlCalPath;
    }

    public String getIntermediateCalEventSessionPath() {
        return IntermediateCalEventSessionPath;
    }

    public void setIntermediateCalEventSessionPath(String intermediateCalEventSessionPath) {
        IntermediateCalEventSessionPath = intermediateCalEventSessionPath;
    }

    public List<String> getInputFillPath(String partitionDate){
        List<String> inputs = new ArrayList<String>();

        inputs.add(IntermediateCalEventSessionPath + partitionDate);
        inputs.add(odlCalPath + partitionDate);

        return inputs;
    }

    public String getOutputFilePath(String partitionDate) {
        return calEventPath + partitionDate;
    }

    public int genCalEvent(String partitionDate) throws URISyntaxException{
        List<String> inputs = getInputFillPath(partitionDate);
        String output = getOutputFilePath(partitionDate);

        boolean ret = HdfsUtils.deleteOutputFilePath(output);
        if (!ret) {
            //说明删除目标目录不成功
//			System.out.print(new Date());
            LogUtils.log2Screen("Delete target diretory faild, don't start mapreduce");
            return -1;
        }

        String[] args = new String[3];
        args[0] = StringUtils.collectionToDelimitedString(inputs, ",");
        args[1] = output;
        args[2] = partitionDate;

        Configuration conf = new Configuration();

        MisProperties properties = MisProperties.getMisProperties(configPath);
        for (Map.Entry<Object, Object> entry : properties.getProps().entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            conf.set(key, value);
        }

        conf.set("gen.cal.event.target.partition", partitionDate);

        int res = -1;
        try {
            res = ToolRunner.run(conf, new GenCalEventMR(), args);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return res;
    }

}
