package lufax.mis.cal.odlmr.action;

import lufax.mis.cal.common.MisProperties;
import lufax.mis.cal.odlmr.mr.FillCalEventGuidMR;
import lufax.mis.cal.utils.HdfsUtils;
import lufax.mis.cal.utils.LogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by lufax on 7/2/15.
 */
public class FillCalEventGuidAction {

//    private String IntermediateCalEventPath = "hdfs://lufaxhd/user/mis/intermediate/fill_cal_event_guid/";
//    private String odlCalPath = "hdfs://lufaxhd/user/mis/etl/cal_log/";

    private String IntermediateCalEventPath = "hdfs://ha-service1/user/mis/intermediate/fill_cal_event_guid/";
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
        return IntermediateCalEventPath + partitionDate;
    }


    public int fillCalEventGuid(String partitionDate) throws URISyntaxException {
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

        conf.set("fill.cal.event.guid.target.partition", partitionDate);

        String homePageCacheFileFormat = properties.getProps().getProperty("fill.cal.guid.illegal.homepage.file.format");
        if (homePageCacheFileFormat != null && !homePageCacheFileFormat.isEmpty()) {
            String cacheFilePath = String.format(homePageCacheFileFormat, partitionDate);
            System.out.println("Cache file path is " + cacheFilePath);

            //TODO 这里需要执行一下hive,执行完毕以后将文件放到HDFS中
            DistributedCache.addCacheFile(new URI(cacheFilePath), conf);
        }

        int res = -1;
        try {
            res = ToolRunner.run(conf, new FillCalEventGuidMR(), args);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        //TODO 这里需要删除一下CacheFile

        return res;
    }
}
