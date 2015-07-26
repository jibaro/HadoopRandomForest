package lufax.mis.cal.odlmr.mr;


import lufax.mis.cal.common.MisCommon;
import lufax.mis.cal.common.record.BackFillOdlCalEventRecord;
import lufax.mis.cal.common.record.OdlCalLogRecord;
import lufax.mis.cal.odlmr.mr.asist.KeyTimeComparator;
import lufax.mis.cal.odlmr.mr.asist.KeyTimeGroupComparator;
import lufax.mis.cal.odlmr.mr.asist.KeyTimePair;
import lufax.mis.cal.odlmr.mr.asist.KeyTimePartitioner;
import lufax.mis.cal.utils.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.*;

/**
 * Created by geyanhao801 on 7/16/15.
 */

public class FillCalEventGuidMR extends Configured implements Tool {
    public class FillCalEventGuidMapper extends Mapper<LongWritable,Text,KeyTimePair,Text> {
        private KeyTimePair outKey = null;
        private Text outValue = new Text();


        private Set<String > illegalGuidSet = new HashSet<String>();
        private Set<String > illegalIpSet = new HashSet<String>();
        private Set<String > illegalHomepageGuidSet = new HashSet<String>();


        //pay attention to the use of substring(int x)
        private boolean handleIllegalCal(String line){
            String[] cols = StringUtils.splitPreserveAllTokens(line,"\t");
            String key = cols[0];

            if (key.startsWith("guid_")){
                String guid = key.substring(5);
                if(!guid.isEmpty()){
                    illegalGuidSet.add(guid);
                }
            }else if (key.startsWith("ip_")){
                String ip = key.substring(3);
                if (!ip.isEmpty()){
                    illegalIpSet.add(ip);
                }
            }
            return true;
        }

        private boolean loadIllegalCal(Configuration conf) throws IOException,ParseException {
            System.out.println("Load Illegal Cal");
            String targetPartition = conf.get("fill.cal.event.guid.target.partition");
            String illegalDirectoryFormat = conf.get("cal.user.link.illegal.directory.format");

            if (illegalDirectoryFormat == null || illegalDirectoryFormat.isEmpty()) {
                return false;
            }

            String illegalDirecotryPath = String.format(illegalDirectoryFormat, targetPartition);

            Path path = new Path(illegalDirecotryPath);
            FileSystem fs = FileSystem.get(conf);

            if(!fs.exists(path)){
                return false;
            }

            if(fs.isFile(path)){
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                String line;
                line = reader.readLine();

                while(line != null){
                    handleIllegalCal(line);
                    line = reader.readLine();
                }
                reader.close();
            }else if (fs.isDirectory(path)){
                FileStatus[] fileStatuses = fs.listStatus(path);

                for (FileStatus fileStatus :fileStatuses){
                    Path childPath = fileStatus.getPath();

                    if (fs.isFile(childPath)){
                        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                        String line;
                        line = reader.readLine();

                        while(line != null){
                            handleIllegalCal(line);
                            line = reader.readLine();
                        }
                        reader.close();
                    }
                }
            }
            return true;
        }

        private boolean loadHomepageGuid(Configuration conf) throws IOException {
            System.out.println("load Homepage Guid");
            String targetPartition = conf.get("fill.cal.event.guid.target.partition");
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);

            for (Path cachaFile : cacheFiles) {
                String fileName = cachaFile.getName();
                System.out.println("Cache file name is " + fileName);

                if (fileName.equals(String.format("bad_home_page_guid.%s.txt", targetPartition))) {
                    BufferedReader buffreader = new BufferedReader(new FileReader(cachaFile.toString()));
                    String line;

                    while ((line = buffreader.readLine()) != null) {
                        String[] cols = StringUtils.splitPreserveAllTokens(line, "\t");

                        if (cols.length > 3) {
                            String tid = cols[0];
                            String guid = cols[1];

                            illegalHomepageGuidSet.add(String.format(("%s_%s"), tid, guid));
                        }
                    }
                }
            }
            return true;
        }
        private boolean initSession2UserId(Configuration conf) throws IOException, ParseException {
            System.out.println("Init Session to UserId");
            String targetPartition = conf.get("fill.cal.event.guid.target.partition");
            Date partitionDate = DateUtils.parseDate(targetPartition);

            String tSessionPathPrefix = conf.get("tsession.path.prefix");
            int sessionInterval = Integer.parseInt(conf.get("fill.cal.event.guid.tsession.interval"));
            int sessionUnit = Integer.parseInt(conf.get("fill.cal.event.guid.tsession.unit"));

            return TSessionUtils.initTSessionMap(conf, tSessionPathPrefix, partitionDate, sessionInterval, sessionUnit);
        }

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            super.setup(context);
            try{
                initSession2UserId(context.getConfiguration());
                loadIllegalCal(context.getConfiguration());
                loadHomepageGuid(context.getConfiguration());
            }catch (ParseException e){
                e.printStackTrace();
                throw new InterruptedException(e.getLocalizedMessage());
            }
        }

        private KeyTimePair generateOutKey(String remoteAddr,String userAgent,Long timestamp){
            KeyTimePair keyTimePair = new KeyTimePair();

            keyTimePair.setKey(MD5Utils.getMD5(String.format("%s_%s",remoteAddr,userAgent)));
            keyTimePair.setTimestamp(timestamp);
            return keyTimePair;
        }

        public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
            OdlCalLogRecord record = new OdlCalLogRecord();
            record.setValue(value.toString());

            if (record.getIsLegal()) {
                context.getCounter("INFO", "LEGAL_CAL_LOG").increment(1);
            }

            //parse Url
            int urlType = UrlUtils.getUrlType(record.getUrl());
            //if not cal nor perf then pass
            if (urlType == MisCommon.OTHER){
                context.getCounter("INFO", "OTHER_LOG").increment(1);
                return;
            }

            BackFillOdlCalEventRecord backcalEvent = new BackFillOdlCalEventRecord();
            backcalEvent.setLogID(record.getLogID());
            backcalEvent.setRemoteAddr(record.getRemoteAddr());
            backcalEvent.setGuid(record.getGuid());
            backcalEvent.setUrlType(urlType);

            String userID = TSessionUtils.getUserIDBySession(record.getLufaxSid());
            backcalEvent.setUserID(userID);


            backcalEvent.setAbnormalType(0);
            backcalEvent.setOrderInSession(0);
            backcalEvent.setTimestamp(record.getTimestamp());


            if (backcalEvent.getGuid().isEmpty()) {
                context.getCounter("DEBUG", "EMPTY_GUID").increment(1);
            }

            if(!record.getGuid().isEmpty() && illegalGuidSet.contains(record.getGuid())){
                context.getCounter("INFO", "ILLEGAL_GUID").increment(1);
                backcalEvent.setAbnormalType(1);

                outKey = generateOutKey(record.getRemoteAddr(), record.getUserAgent(), record.getTimestamp());
                outValue.set(backcalEvent.getValue());
                context.write(outKey, outValue);
            }else if (!record.getRemoteAddr().isEmpty() && illegalIpSet.contains(record.getRemoteAddr())){
                context.getCounter("INFO" , "ILLEGAL_IP").increment(1);
                backcalEvent.setAbnormalType(1);

                outKey = generateOutKey(record.getRemoteAddr(), record.getUserAgent(), record.getTimestamp());
                outValue.set(backcalEvent.getValue());
                context.write(outKey, outValue);
            }else if (!record.getGuid().isEmpty() && !record.getTid().isEmpty() && illegalHomepageGuidSet.contains(String.format("%s_%s", record.getTid(), record.getGuid()))){
                context.getCounter("INFO", "ILLEGAL_HOMEPAGE").increment(1);
                backcalEvent.setAbnormalType(2);

                outKey = generateOutKey(record.getRemoteAddr(), record.getUserAgent(), record.getTimestamp());
                outValue.set(backcalEvent.getValue());
                context.write(outKey, outValue);
            }else{
                context.getCounter("INFO", "LEGAL_EVENT").increment(1);
                //abnormal type不是0的不予已补填

                outKey = generateOutKey(record.getRemoteAddr(), record.getUserAgent(), record.getTimestamp());
                outValue.set(backcalEvent.getValue());

                System.out.println("out key is " + outKey.toString());
                context.getCounter("DEBUG", "KEY_" + outKey.toString()).increment(1);

                context.write(outKey, outValue);
            }
        }
    }
    public static class FillCalEventGuidReducer extends Reducer<KeyTimePair,Text,LongWritable,Text> {
        private LongWritable outKey = new LongWritable();
        private Text outValue = new Text();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        public void reduce(KeyTimePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<BackFillOdlCalEventRecord> unhandledBackFillOdlCalEvent = new ArrayList<BackFillOdlCalEventRecord>();

            String lastguid = "";
            String nearguid = "";

            for (Text value : values) {
                BackFillOdlCalEventRecord backRecord = new BackFillOdlCalEventRecord();
                backRecord.setValue(value.toString());

                if (backRecord.getGuid().isEmpty()) {
                    context.getCounter("DEBUG", "EMPTY_GUID1").increment(1);
                }

                //do not fill when Abnormal Type is null
                if (backRecord.getAbnormalType() != 0) {
                    context.getCounter("INFO", "ABNORMAL_" + backRecord.getAbnormalType()).increment(1);

                    outKey.set(backRecord.getKey());
                    outValue.set(backRecord.getValue());
                    context.write(outKey, outValue);

                    continue;
                }

                //do not fill when Remote Addr is null
                if (backRecord.getRemoteAddr().isEmpty()) {
                    context.getCounter("INFO", "EMPTY_IP").increment(1);

                    outKey.set(backRecord.getKey());
                    outValue.set(backRecord.getValue());
                    context.write(outKey, outValue);

                    continue;
                }

                if (backRecord.getGuid().isEmpty()) {
                    context.getCounter("DEBUG", "EMPTY_GUID2").increment(1);
                }

                context.getCounter("INFO", "NORMAL_CAL_EVENT").increment(1);

                if (!backRecord.getGuid().isEmpty() && lastguid.isEmpty()) {
                    lastguid = backRecord.getGuid();
                    System.out.println("last guid is " + lastguid);
                }

                //一旦找到guid，更新nearGuid
                if (!backRecord.getGuid().isEmpty()) {
                    nearguid = backRecord.getGuid();
                    System.out.println("near guid is " + nearguid);
                }

                if (nearguid.isEmpty()) {
                    unhandledBackFillOdlCalEvent.add(backRecord);
                    continue;
                }

                if (backRecord.getGuid().isEmpty()) {
                    context.getCounter("DEBUG", "EMPTY_GUID3").increment(1);
                }

                backRecord.setGuidPost(nearguid);
                outKey.set(backRecord.getKey());
                outValue.set(backRecord.getValue());
                context.write(outKey, outValue);
            }

            if (!lastguid.isEmpty()) {
                for (BackFillOdlCalEventRecord record : unhandledBackFillOdlCalEvent) {
                    record.setGuidPost(lastguid);
                    outKey.set(record.getKey());
                    outValue.set(record.getValue());
                    context.write(outKey, outValue);
                }
            } else {
                for (BackFillOdlCalEventRecord record : unhandledBackFillOdlCalEvent) {
                    context.getCounter("INFO", "UNBACK_FILL_GUID").increment(1);

                    outKey.set(record.getKey());
                    outValue.set(record.getValue());
                    context.write(outKey, outValue);
                }

            }
        }
    }
    public int run(String[] args) throws Exception {
        String inputs = args[0];
        String outputPath = args[1];
        String partitionDate = args[2];

        LogUtils.log2Screen("inputPath is " + inputs);
        LogUtils.log2Screen("outputPath is " + outputPath);

        Configuration conf = this.getConf();
//		conf.set("mapred.job.queue.name", "root.mis.mis_mr");
//		conf.set("mapred.reduce.slowstart.completed.maps", "0.8");
        conf.set("mapred.child.java.opts", "-Xmx4096m");
        conf.set("mapred.output.compress", "true");
        conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("mapred.job.priority", "VERY_HIGH");
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.reduce.memory.mb", "4096");
//		conf.set("hadoop.tmp.dir", "/wls/applications/loki-app/tmp");

        Job job = new Job(conf, "FillCalEventGuid@" + partitionDate);
        job.setJarByClass(FillCalEventGuidMR.class);

        job.setPartitionerClass(KeyTimePartitioner.class);
        job.setGroupingComparatorClass(KeyTimeGroupComparator.class);
        job.setSortComparatorClass(KeyTimeComparator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(FillCalEventGuidMapper.class);
        job.setMapOutputKeyClass(KeyTimePair.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(FillCalEventGuidReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

//        job.setReducerClass(FillCalEventGuidTestReducer.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        String[] inputArray = inputs.split(",");
        for (String inputPath : inputArray) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
