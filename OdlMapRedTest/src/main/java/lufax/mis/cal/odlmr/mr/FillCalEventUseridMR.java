package lufax.mis.cal.odlmr.mr;

import lufax.mis.cal.common.record.BackFillOdlCalEventRecord;
import lufax.mis.cal.odlmr.mr.asist.KeyTimeComparator;
import lufax.mis.cal.odlmr.mr.asist.KeyTimeGroupComparator;
import lufax.mis.cal.odlmr.mr.asist.KeyTimePair;
import lufax.mis.cal.odlmr.mr.asist.KeyTimePartitioner;
import lufax.mis.cal.utils.LogUtils;
import lufax.mis.cal.utils.MD5Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by geyanhao801 on 7/16/15.
 */
public class FillCalEventUseridMR extends Configured implements Tool{
    public static class FillCalEventUseridMapper extends Mapper<Text,Text,KeyTimePair,Text> {
        private KeyTimePair outKey = null;
        private Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        public KeyTimePair generateKey(String guid, long timestamp) {
            KeyTimePair keyTimePair = new KeyTimePair();
            keyTimePair.setKey(MD5Utils.getMD5(guid));
            keyTimePair.setTimestamp(timestamp);

            return keyTimePair;
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            BackFillOdlCalEventRecord record = new BackFillOdlCalEventRecord();
            record.setValue(value.toString());

            outKey = generateKey(record.getGuidPost(), record.getTimestamp());
            context.write(outKey, value);

//            }
        }
    }
    public static class FillCalEventUseridReducer extends Reducer<KeyTimePair,Text,LongWritable,Text> {
        private LongWritable outKey = new LongWritable();
        private Text outValue = new Text();
        private Map<String, String> userLinkingMap = new HashMap<String, String>();

        private boolean loadUserLinking() {

            return true;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        public void reduce(KeyTimePair key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
            List<BackFillOdlCalEventRecord> unableHandledCalEvents = new ArrayList<BackFillOdlCalEventRecord>();

            String lastUserID = "";
            String nearUserID = "";

            for (Text value : values) {
                BackFillOdlCalEventRecord record = new BackFillOdlCalEventRecord();
                record.setValue(value.toString());
                System.out.println("value is [" + value.toString() + "]");

                //对于Abnormal Type不为0的数据，不予已补填
                if (record.getAbnormalType() != 0) {
                    context.getCounter("INFO", "ABNORMAL_" + record.getAbnormalType()).increment(1);

                    outKey.set(record.getKey());
                    outValue.set(record.getValue());
                    context.write(outKey, outValue);
                    continue;
                }

                //对于没有guid post的数据，不予已补填
                if (record.getGuidPost().isEmpty()) {
                    context.getCounter("INFO", "EMPTY_GUID").increment(1);

                    outKey.set(record.getKey());
                    outValue.set(record.getValue());
                    context.write(outKey, outValue);
                    continue;
                }

                context.getCounter("INFO", "NORMAL_CAL_EVENT").increment(1);

                //将找到的第一个userID作为last userID
                if (!record.getUserID().isEmpty() && lastUserID.isEmpty()) {
                    lastUserID = record.getUserID();
                    System.out.println("last user id is " + lastUserID);
                }

                //一旦找到user id，更新near userID
                if (!record.getUserID().isEmpty()) {
                    nearUserID = record.getUserID();
                    System.out.println("near user id is " + nearUserID);
                }

                //如果无法找到相应的user id可以用来补填这条calEvent，则放到unableHandledCalEvent中
                if (nearUserID.isEmpty()) {
                    unableHandledCalEvents.add(record);
                    continue;
                }

                context.getCounter("INFO", "BACK_FILL_USERID1").increment(1);
                record.setUserIDPost(nearUserID);
                outKey.set(record.getKey());
                outValue.set(record.getValue());
                context.write(outKey, outValue);
            }
            if(!lastUserID.isEmpty()){
                for(BackFillOdlCalEventRecord record :unableHandledCalEvents){
                    context.getCounter("INFO", "BACK_FILL_USERID2").increment(1);
                    record.setUserIDPost(lastUserID);

                    outKey.set(record.getKey());
                    outValue.set(record.getValue());
                    context.write(outKey, outValue);
                }
            }else {
                loadUserLinking();
                for (BackFillOdlCalEventRecord record : unableHandledCalEvents) {
                    if (userLinkingMap.containsKey(record.getGuidPost())) {
                        context.getCounter("INFO", "BACK_FILL_USERID3").increment(1);
                        record.setUserIDPost(userLinkingMap.get(record.getGuidPost()));

                        outKey.set(record.getKey());
                        outValue.set(record.getValue());
                        context.write(outKey, outValue);
                    } else {
                        context.getCounter("INFO", "UNBACK_FILL_USERID").increment(1);

                        outKey.set(record.getKey());
                        outValue.set(record.getValue());
                        context.write(outKey, outValue);
                    }
                }
            }
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        String inputs = args[0];
        String outputPath = args[1];
        String partitionDate = args[2];

        LogUtils.log2Screen("inputPath is " + inputs);
        LogUtils.log2Screen("outputPath is " + outputPath);

        Configuration conf = this.getConf();
        conf.set("mapred.child.java.opts", "-Xmx4096m");
        conf.set("mapred.output.compress", "true");
        conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("mapred.job.priority", "VERY_HIGH");
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.reduce.memory.mb", "4096");
//		conf.set("hadoop.tmp.dir", "/wls/applications/loki-app/tmp");

        Job job = new Job(conf, "FillCalEventUserID@" + partitionDate);
        job.setJarByClass(FillCalEventUseridMR.class);

        job.setPartitionerClass(KeyTimePartitioner.class);
        job.setGroupingComparatorClass(KeyTimeGroupComparator.class);
        job.setSortComparatorClass(KeyTimeComparator.class);

        job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
        job.setMapperClass(FillCalEventUseridMapper.class);
        job.setMapOutputKeyClass(KeyTimePair.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(FillCalEventUseridReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
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
