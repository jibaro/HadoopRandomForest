package lufax.mis.cal.odlmr.mr;

import lufax.mis.cal.common.MisCommon;
import lufax.mis.cal.common.record.OdlCalLogRecord;
import lufax.mis.cal.utils.DateUtils;
import lufax.mis.cal.utils.LogUtils;
import lufax.mis.cal.utils.TSessionUtils;
import lufax.mis.cal.utils.UrlUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;


/**
 * Created by geyanhao801 on 7/14/15.
 */

public class GenUserLinkingMR extends Configured implements Tool {
    private static final String ILLEGAL = "illegal";

    public static class GenUserLinkingMapper extends Mapper<LongWritable,Text,Text,Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();


        private boolean initSession2UserId(Configuration conf) throws IOException, ParseException {
            String targetPartition = conf.get("user.linking.target.partition");
            Date partitionDate = DateUtils.parseDate(targetPartition);

            String tSessionPathPrefix = conf.get("tsession.path.prefix");
            int sessionInterval = Integer.parseInt(conf.get("user.linking.tsession.interval"));
            int sessionUnit = Integer.parseInt(conf.get("user.linking.tsession.unit"));

            return TSessionUtils.initTSessionMap(conf, tSessionPathPrefix, partitionDate, sessionInterval, sessionUnit);
        }

        @Override
        public void setup(Context context) throws IOException,InterruptedException {
            super.setup(context);
            /**
            try {
                initSession2UserId(context.getConfiguration());
            } catch (ParseException e) {
                e.printStackTrace();
                throw new InterruptedException(e.getLocalizedMessage());
            }**/
        }

        @Override
        public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
            OdlCalLogRecord record = new OdlCalLogRecord();
            record.setValue(value.toString());

            if (record.getIsLegal()) {
                context.getCounter("INFO", "LEGAL_CAL_LOG").increment(1);
            }

            int urlType = UrlUtils.getUrlType(record.getUrl());
            if (urlType == MisCommon.OTHER) {
                context.getCounter("INFO", "OTHER_LOG").increment(1);
                return;
            }

            String guid = record.getGuid();
            String ip = record.getRemoteAddr();
            String ua = record.getUserAgent();


            if (!guid.isEmpty()) {
                outKey.set("guid_" + guid);
                outValue.set("1");
                context.write(outKey, outValue);
            }

            if (!ip.isEmpty()) {
                outKey.set("ip_" + ip);
                outValue.set(ua);
                context.write(outKey, outValue);
            }

            String lufaxSid = record.getLufaxSid();
            if (lufaxSid.isEmpty()) {
                return;
            }

            context.getCounter("DEBUG", "sid_" + lufaxSid).increment(1);

            String userID = TSessionUtils.getUserIDBySession(lufaxSid);
            if (!lufaxSid.isEmpty() && !record.getGuid().isEmpty()) {
                System.out.println(record.getGuid() + " <------> " + lufaxSid);
                outKey.set(record.getGuid());

                outValue.set(lufaxSid + "\t" + record.getTimestamp());
                context.write(outKey, outValue);
            }
        }
    }

    public static class GenUserLinkingReducer extends Reducer<Text,Text,Text,Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        //Use MultipleOutputs
        private MultipleOutputs<Text, Text> illegalMos = null;
        private String illegalOutputPathPrefix = "";

        private long guidMaxPvLimit = 0l;
        private long ipMaxUaLimit = 0l;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            String guidMaxPvStr = context.getConfiguration().get("guid.max.pv.limit");
            if (guidMaxPvStr != null && !guidMaxPvStr.isEmpty()) {
                guidMaxPvLimit = Long.parseLong(guidMaxPvStr);
            }

            String ipMaxUaStr = context.getConfiguration().get("ip.max.ua.limit");
            if (ipMaxUaStr != null && !ipMaxUaStr.isEmpty()) {
                ipMaxUaLimit = Long.parseLong(ipMaxUaStr);
            }

            String targetPartition = context.getConfiguration().get("user.linking.target.partition");
            String illegalOutputPathPrefixFormat = context.getConfiguration().get("cal.user.link.illegal.path.prefix.format");

            illegalOutputPathPrefix = String.format(illegalOutputPathPrefixFormat, targetPartition);
            illegalMos = new MultipleOutputs<Text, Text>(context);
        }

        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
            //logID是key，理论上logID是全局唯一的，所以如果logID有重合的话，只输出一条
            String keyStr = key.toString();

            if(guidMaxPvLimit > 0 && keyStr.startsWith("guid_")){
                long pv = 0l;

                for (Text value : values) {
                    pv+=Long.parseLong(value.toString());

                    if( pv > guidMaxPvLimit ){
                        context.getCounter("DEBUG","ILLEGAL_GUID_1").increment(1);
                        illegalMos.write(GenUserLinkingMR.ILLEGAL,key,new Text(String.valueOf(pv)),illegalOutputPathPrefix);
                        context.getCounter("DEBUG","ILLEGAL_GUID_2").increment(1);
                        return;
                    }

                }
            }else if (ipMaxUaLimit > 0 && keyStr.startsWith("ip_")) {
                Set<String> uaSet = new HashSet<String>();

                for (Text value : values) {
                    uaSet.add(value.toString());

                    if (uaSet.size() >= ipMaxUaLimit) {
                        context.getCounter("DEBUG", "ILLEGAL_IP_1").increment(1);
                        illegalMos.write(GenUserLinkingMR.ILLEGAL, key, new Text(String.valueOf(uaSet.size())), illegalOutputPathPrefix);
                        context.getCounter("DEBUG", "ILLEGAL_IP_2").increment(1);
                    }
                }
            }else{
                String guid = keyStr;
                Long maxTimestamp = 0l;
                String userID = "";

                for (Text value : values){
                    String[] cols = StringUtils.splitPreserveAllTokens(value.toString(), "\t");

                    String currentUserID = cols[0];
                    long timestamp = Long.parseLong(cols[1]);

                    if(timestamp > maxTimestamp && !currentUserID.isEmpty()){
                        userID = currentUserID;
                        maxTimestamp = timestamp;
                    }
                }
                outKey.set(guid);
                outValue.set(userID);
                context.write(outKey, outValue);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException,InterruptedException {
            super.cleanup(context);
            illegalMos.close();
        }
    }

    //这个输出的文件都是需要读入内存的，所以暂时不写成sequence file
    public int run(String[] args) throws Exception {
        String inputs = args[0];
        String outputsPath = args[1];
        String partitionDate = args[2];

        LogUtils.log2Screen("inputPath is " + inputs);
        LogUtils.log2Screen("outputPath is " + outputsPath);

        Configuration conf = this.getConf();
        conf.set("mapred.job.priority", "VERY_HIGH");
        conf.set("mapred.child.java.opts", "-Xmx2048m");
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("user.linking.target.partition" ,"2015-07-10");
        conf.set("mapred.child.java.opts", "-Xmx2048m");
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapred.job.priority", "VERY_HIGH");
        conf.set("mapred.child.java.opts", "-Xmx2048m");
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.reduce.memory.mb", "4096");



        Job job = new Job(conf,"GenUserLinking@"+partitionDate);
        job.setJarByClass(GenUserLinkingMR.class);

        MultipleOutputs.addNamedOutput(job,GenUserLinkingMR.ILLEGAL,TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.setCountersEnabled(job,true);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(GenUserLinkingMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(GenUserLinkingReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        String[] inputArray = inputs.split(",");
        for (String inputPath : inputArray){
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(outputsPath));
        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new GenUserLinkingMR(),args);
        System.exit(res);
    }
}



