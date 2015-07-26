package lufax.mis.cal.odlmr.mr;

import lufax.mis.cal.common.MisCommon;
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
import java.util.List;

/**
 * Created by geyanhao801 on 7/16/15.
 */
public class GenCalEventSessionMR extends Configured implements Tool{
    //这个参数可以考虑从配置文件读入
    private final static int SESSION_INTERVAL = 900 * 1000;

    public static class GenCalEventSessionMapper extends Mapper<Text,Text,KeyTimePair,Text>{
        private KeyTimePair outKey = new KeyTimePair();
        private Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            super.setup(context);
        }

        public KeyTimePair generateKey(String guid, long timestamp) {
            KeyTimePair keyTimePair = new KeyTimePair();
            keyTimePair.setKey(MD5Utils.getMD5(guid));
            keyTimePair.setTimestamp(timestamp);

            return keyTimePair;
        }

        @Override
        public void map(Text key,Text value,Context context) throws IOException,InterruptedException {
            BackFillOdlCalEventRecord backRecord  = new BackFillOdlCalEventRecord();
            backRecord.setValue(value.toString());

            outKey = generateKey(backRecord.getGuidPost(), backRecord.getTimestamp());
            context.write(outKey, value);
        }
    }

    public static class GenCalEventSessionReducer extends Reducer<KeyTimePair,Text,Text,Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }


        private boolean isSameSession(BackFillOdlCalEventRecord after, BackFillOdlCalEventRecord previous) {
            if (previous == null) {
                return false;
            }

            if (!previous.getUserIDPost().equalsIgnoreCase(after.getUserIDPost())) {
                return false;
            }

            if ((after.getTimestamp() - previous.getTimestamp()) > GenCalEventSessionMR.SESSION_INTERVAL) {
                return false;
            }

            return true;
        }

        @Override
        protected void reduce(KeyTimePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<BackFillOdlCalEventRecord> session = new ArrayList<BackFillOdlCalEventRecord>();
            Long sessionID = 0l;
            Integer orderInSession = 0;
            Integer orderInCalSession = 0;
            Integer orderInPerfSession = 0;

            BackFillOdlCalEventRecord previousRecord = null;

            for (Text value : values) {
                BackFillOdlCalEventRecord record = new BackFillOdlCalEventRecord();
                record.setValue(value.toString());

                //对于Abnormal Type不为0的数据，不予已session化
                if (record.getAbnormalType() != 0) {
                    context.getCounter("INFO", "ABNORMAL_" + record.getAbnormalType()).increment(1);

                    outKey.set(String.valueOf(record.getKey()));
                    outValue.set(record.getValue());
                    context.write(outKey, outValue);
                    continue;
                }

                if (record.getGuidPost().isEmpty()) {
                    context.getCounter("INFO", "EMPTY_GUID").increment(1);

                    outKey.set(String.valueOf(record.getKey()));
                    outValue.set(record.getValue());
                    context.write(outKey, outValue);
                    continue;
                }
                //全局Session
                if (!isSameSession(record, previousRecord)) {
                    sessionID = record.getLogID();
                    orderInSession = 0;
                    orderInCalSession = 0;
                    orderInPerfSession = 0;
                }

                previousRecord = record;

                orderInSession += 1;
                record.setSessionID(sessionID);
                record.setOrderInSession(orderInSession);

                //处理Cal Session
                if (record.getUrlType() == MisCommon.CAL) {
                    context.getCounter("INFO", "CAL_SESSION").increment(1);

                    orderInCalSession += 1;
                    record.setOrderInCalSession(orderInCalSession);
                } else if (record.getUrlType() == MisCommon.PERF) {
                    context.getCounter("INFO", "PERF_SESSION").increment(1);

                    orderInPerfSession += 1;
                    record.setOrderInPerfSession(orderInPerfSession);
                }

                outKey.set(String.valueOf(record.getKey()));
                outValue.set(record.getValue());
                context.write(outKey, outValue);
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
            conf.set("mapred.child.java.opts", "-Xmx4096m");
            conf.set("mapred.output.compress", "true");
            conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
            conf.set("mapred.job.priority", "VERY_HIGH");
            conf.set("mapreduce.map.memory.mb", "2048");
            conf.set("mapreduce.reduce.memory.mb", "4096");

            Job job = new Job(conf, "GenCalEventSession@" + partitionDate);
            job.setJarByClass(GenCalEventSessionMR.class);

            job.setPartitionerClass(KeyTimePartitioner.class);
            job.setGroupingComparatorClass(KeyTimeGroupComparator.class);
            job.setSortComparatorClass(KeyTimeComparator.class);

            job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
            job.setMapperClass(GenCalEventSessionMapper.class);
            job.setMapOutputKeyClass(KeyTimePair.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(GenCalEventSessionReducer.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

//          job.setReducerClass(FillCalEventGuidTestReducer.class);
//          job.setOutputFormatClass(SequenceFileOutputFormat.class);
//          job.setOutputKeyClass(Text.class);
//          job.setOutputValueClass(Text.class);

            job.setNumReduceTasks(1);

            String[] inputArray = inputs.split(",");
            for (String inputPath : inputArray) {
                FileInputFormat.addInputPath(job, new Path(inputPath));
            }
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            return job.waitForCompletion(true) ? 0 : 1;
        }
}


