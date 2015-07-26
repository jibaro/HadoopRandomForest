package lufax.mis.cal.odlmr.action;

import lufax.mis.cal.common.MisProperties;
import lufax.mis.cal.odlmr.mr.GenUserLinkingMR;
import lufax.mis.cal.utils.HdfsUtils;
import lufax.mis.cal.utils.LogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by lufax on 7/2/15.
 */
public class GenUserLinkingAction {
    private String userLinkingPath = "hdfs://ha-service1/user/mis/hdw/user_linking/";
    private String odlCalPath = "hdfs://ha-service1/user/mis/etl/cal_log/";

    private String configPath = "";

    public String getOdlCalPath() {
        return odlCalPath;
    }

    public void setOdlCalPath(String odlCalPath) {
        this.odlCalPath = odlCalPath;
    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public String getInputFilePath(String partitionDate) {
        return odlCalPath + partitionDate;
    }

    public String getOutputFilePath(String partitionDate) {
        return userLinkingPath + partitionDate;
    }

    public String getMultiOutputPath(MisProperties properties, String partitionDate) {
        String multiOutputFomat = properties.getProps().getProperty("cal.user.link.illegal.directory.format");

        if (multiOutputFomat != null && !multiOutputFomat.isEmpty()) {
            return String.format(multiOutputFomat, partitionDate);
        }

        return "";
    }

    public int genUserLinking(String partitionDate) {
        MisProperties properties = MisProperties.getMisProperties(configPath);

        List<String> inputs = new ArrayList<String>();
        inputs.add(getInputFilePath(partitionDate));

        String output = getOutputFilePath(partitionDate);
        String multiOutputPath = getMultiOutputPath(properties, partitionDate);
        if (!multiOutputPath.isEmpty()) {
            System.out.println("multiOutputPath is " + multiOutputPath);

            boolean ret = HdfsUtils.deleteOutputFilePath(multiOutputPath);
            if (!ret) {
                //说明删除目标目录不成功
                LogUtils.log2Screen("Delete multiple output diretory faild, don't start mapreduce");
                return -1;
            }
        }

        boolean ret = HdfsUtils.deleteOutputFilePath(output);
        if (!ret) {
            //说明删除目标目录不成功
            LogUtils.log2Screen("Delete target diretory faild, don't start mapreduce");
            return -1;
        }

        String[] args = new String[3];
        args[0] = StringUtils.collectionToDelimitedString(inputs, ",");
        args[1] = output;
        args[2] = partitionDate;

        Configuration conf = new Configuration();

        for (Map.Entry<Object, Object> entry : properties.getProps().entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            conf.set(key, value);
        }
        conf.set("user.linking.target.partition", partitionDate);


        int res = -1;
        try {
            res = ToolRunner.run(conf, new GenUserLinkingMR(), args);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return res;
    }

}
