package lufax.mis.cal.odlmr.action;

import lufax.mis.cal.common.record.BackFillOdlCalEventRecord;
import lufax.mis.cal.common.record.OdlCalLogRecord;
import lufax.mis.cal.odlmr.mr.asist.KeyTimeComparator;
import lufax.mis.cal.odlmr.mr.asist.KeyTimeGroupComparator;
import lufax.mis.cal.odlmr.mr.asist.KeyTimePair;
import lufax.mis.cal.odlmr.mr.asist.KeyTimePartitioner;
import lufax.mis.cal.utils.LogUtils;
import lufax.mis.cal.utils.MD5Utils;
import lufax.mis.cal.utils.TSessionUtils;
import lufax.mis.cal.utils.UrlUtils;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by geyanhao801 on 7/13/15.
 */


public class GenCalEventLogTest extends Configured implements Tool{
    public static class GenCalEventLogTestMapper extends Mapper<LongWritable,Text,KeyTimePair,Text> {
        private KeyTimePair outkey = new KeyTimePair();
        private Text outValue = new Text();

        private KeyTimePair generateOutKey(String remoteAddr, String userAgent, Long timestamp) {
            KeyTimePair keyTimePair = new KeyTimePair();

            keyTimePair.setKey(MD5Utils.getMD5(String.format("%s_%s", remoteAddr, userAgent)));
            keyTimePair.setTimestamp(timestamp);
            return keyTimePair;
        }

        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            OdlCalLogRecord odlCalLogRecord = new OdlCalLogRecord();
            odlCalLogRecord.setValue(value.toString());

            BackFillOdlCalEventRecord backFillOdlCalEventRecord = new BackFillOdlCalEventRecord();
            backFillOdlCalEventRecord.setLogID(odlCalLogRecord.getLogID());
            backFillOdlCalEventRecord.setRemoteAddr(odlCalLogRecord.getRemoteAddr());
            backFillOdlCalEventRecord.setGuid(odlCalLogRecord.getGuid());
            //parse Url
            int urlType = UrlUtils.getUrlType(odlCalLogRecord.getUrl());
            //if not cal nor perf then pass
            /**
            if (urlType == MisCommon.OTHER) {
                context.getCounter("INFO", "OTHER_LOG").increment(1);
                return;
            }**/
            backFillOdlCalEventRecord.setUrlType(urlType);

            String userID = TSessionUtils.getUserIDBySession(odlCalLogRecord.getLufaxSid());
            backFillOdlCalEventRecord.setUserID(userID);
            backFillOdlCalEventRecord.setAbnormalType(0);
            backFillOdlCalEventRecord.setOrderInSession(0);
            backFillOdlCalEventRecord.setTimestamp(odlCalLogRecord.getTimestamp());

            context.getCounter("INFO", "LEGAL_EVENT").increment(1);
            //abnormal type不是0的不予已补填
            outkey = generateOutKey(odlCalLogRecord.getRemoteAddr(), odlCalLogRecord.getUserAgent(), odlCalLogRecord.getTimestamp());
            outValue.set(backFillOdlCalEventRecord.getValue());

            System.out.println("out key is " + outkey.toString());
            context.getCounter("DEBUG", "KEY_" + outValue.toString()).increment(1);
            context.write(outkey, outValue);
        }
    }

    public static class GenCalEventLogTestReducer extends Reducer<KeyTimePair,Iterable<Text>,LongWritable,Text> {
        private LongWritable outKey = new LongWritable();
        private Text outValue = new Text();

        public void setup(Context context) throws IOException,InterruptedException{
            super.setup(context);
        }

        public void reduce(KeyTimePair key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
            List<BackFillOdlCalEventRecord> unhandledBackFillOdlCalEventRecord = new ArrayList<BackFillOdlCalEventRecord>();

            String lastguid = "";
            String nearguid = "";

            for (Text value :values){
                BackFillOdlCalEventRecord backFillOdlCalEventRecord = new BackFillOdlCalEventRecord();
                backFillOdlCalEventRecord.setValue(value.toString());
                if (backFillOdlCalEventRecord.getAbnormalType() != 0) {
                    context.getCounter("INFO", "ABNORMAL_" + backFillOdlCalEventRecord.getAbnormalType()).increment(1);

                    outKey.set(backFillOdlCalEventRecord.getKey());
                    outValue.set(backFillOdlCalEventRecord.getValue());
                    context.write(outKey, outValue);

                    continue;
                }

                //do not fill when Remote Addr is null
                if (backFillOdlCalEventRecord.getRemoteAddr().isEmpty()) {
                    context.getCounter("INFO", "EMPTY_IP").increment(1);

                    outKey.set(backFillOdlCalEventRecord.getKey());
                    outValue.set(backFillOdlCalEventRecord.getValue());
                    context.write(outKey, outValue);

                    continue;
                }


                if (backFillOdlCalEventRecord.getGuid().isEmpty()) {
                    context.getCounter("DEBUG", "EMPTY_GUID2").increment(1);
                }

                context.getCounter("INFO", "NORMAL_CAL_EVENT").increment(1);

                if (!backFillOdlCalEventRecord.getGuid().isEmpty() && lastguid.isEmpty()) {
                    lastguid = backFillOdlCalEventRecord.getGuid();
                    System.out.println("last guid is " + lastguid);
                }

                //一旦找到guid，更新nearGuid
                if (!backFillOdlCalEventRecord.getGuid().isEmpty()) {
                    nearguid = backFillOdlCalEventRecord.getGuid();
                    System.out.println("near guid is " + nearguid);
                }

                if (nearguid.isEmpty()) {
                    unhandledBackFillOdlCalEventRecord.add(backFillOdlCalEventRecord);
                    continue;
                }

                if (backFillOdlCalEventRecord.getGuid().isEmpty()) {
                    context.getCounter("DEBUG", "EMPTY_GUID3").increment(1);
                }

                backFillOdlCalEventRecord.setGuidPost(nearguid);
                outKey.set(backFillOdlCalEventRecord.getKey());
                outValue.set(backFillOdlCalEventRecord.getValue());
                context.write(outKey, outValue);
            }

            if (!lastguid.isEmpty()) {
                for (BackFillOdlCalEventRecord record : unhandledBackFillOdlCalEventRecord) {
                    record.setGuidPost(lastguid);
                    outKey.set(record.getKey());
                    outValue.set(record.getValue());
                    context.write(outKey, outValue);
                }
            } else {
                for (BackFillOdlCalEventRecord record : unhandledBackFillOdlCalEventRecord) {
                    context.getCounter("INFO", "UNBACK_FILL_GUID").increment(1);

                    outKey.set(record.getKey());
                    outValue.set(record.getValue());
                    context.write(outKey, outValue);
                }

            }
        }
    }

    public int run(String[] args) throws Exception{
         String inputs = args[0];
         String outputPath = args[1];


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


        Job job = new Job(conf,"GenCalEventLogTest");
        job.setJarByClass(GenCalEventLogTest.class);

        job.setPartitionerClass(KeyTimePartitioner.class);
        job.setGroupingComparatorClass(KeyTimeGroupComparator.class);
        job.setSortComparatorClass(KeyTimeComparator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(GenCalEventLogTestMapper.class);
        job.setMapOutputKeyClass(KeyTimePair.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(GenCalEventLogTestReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        String[] inputArray = inputs.split(",");
        for (String inputPath : inputArray) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int excute = ToolRunner.run(new GenCalEventLogTest(), args);
        System.exit(excute);
    }
}


