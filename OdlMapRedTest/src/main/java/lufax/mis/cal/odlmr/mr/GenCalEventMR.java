package lufax.mis.cal.odlmr.mr;

import lufax.mis.cal.common.MisCommon;
import lufax.mis.cal.common.record.BackFillOdlCalEventRecord;
import lufax.mis.cal.common.record.CalEventRecord;
import lufax.mis.cal.common.record.OdlCalLogRecord;
import lufax.mis.cal.utils.LogUtils;
import lufax.mis.cal.utils.UrlUtils;
import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by geyanhao801 on 7/17/15.
 */
public class GenCalEventMR extends Configured implements Tool{
    private static final String PERF_MOS = "perf";

    public static class GenCalEventMapper extends Mapper<Text,Text,Text,Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void setup(Context context) throws IOException,InterruptedException{
            super.setup(context);
        }

        public void map(Text key,Text value,Context context) throws IOException,InterruptedException {
            String dataSource = StringUtils.splitPreserveAllTokens(value.toString(), "\t")[0];
            context.getCounter("INFO", "DS_" + dataSource).increment(1);

            if(dataSource.equals(OdlCalLogRecord.dataSource)){
                OdlCalLogRecord record = new OdlCalLogRecord();
                record.setValue(value.toString());

                //对于非Cal和非Perf的日志，不需要进行后续处理
                if (UrlUtils.getUrlType(record.getUrl()) == MisCommon.OTHER) {
                    return;
                }
                outKey.set(Long.toString(record.getKey()));
                outValue.set(record.getValue());
                context.write(outKey,outValue);
            }
            else if (dataSource.equals(BackFillOdlCalEventRecord.dataSource)){
                BackFillOdlCalEventRecord record = new BackFillOdlCalEventRecord();
                record.setValue(value.toString());

                outKey.set(Long.toString(record.getKey()));
                outValue.set(record.getValue());
                context.write(outKey,outValue);
            }
        }
    }
    public static class GenCalEventReducer extends Reducer<Text,Text,Text,Text> {
        private Text outkey = new Text();
        private Text outValue = new Text();

        private MultipleOutputs<Text,Text> perfMos = null;
        private String perfOutputPathPrefix = "";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            perfOutputPathPrefix = context.getConfiguration().get("cal.perf.path.prefix");
            perfMos = new MultipleOutputs<Text, Text>(context);
        }

        @Override
        protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
            BackFillOdlCalEventRecord backRecord = new BackFillOdlCalEventRecord();
            OdlCalLogRecord calLog = new OdlCalLogRecord();

            int valueCount = 0;
            for (Text value : values){
                valueCount += 1;
                String dataSource = StringUtils.splitPreserveAllTokens(value.toString(), "\t")[0];
                context.getCounter("INFO", "DS_" + dataSource).increment(1);
                if(dataSource.equals(OdlCalLogRecord.dataSource)){
                    calLog.setValue(value.toString());
                }
                else if (dataSource.equals(BackFillOdlCalEventRecord.dataSource)){
                    backRecord.setValue(value.toString());
                }
            }

            context.getCounter("DEBUG", "VALUE_SIZE_" + valueCount).increment(1);

            CalEventRecord calEventRecord = new CalEventRecord();
            calEventRecord.setValue(calLog,backRecord);

            outkey.set(calEventRecord.getKey());
            outValue.set(calEventRecord.getValue());

            if (calEventRecord.getAbnormalType() != 0) {
                context.getCounter("INFO", "ABNORMAL_" + calEventRecord.getAbnormalType()).increment(1);
            }

            if (backRecord.getUrlType() == MisCommon.CAL) {
                context.getCounter("INFO", "CAL_EVENT").increment(1);
                context.write(outkey, outValue);
            } else if (backRecord.getUrlType() == MisCommon.PERF) {
                context.getCounter("INFO", "PERF").increment(1);
                perfMos.write(GenCalEventMR.PERF_MOS, outkey, outValue, perfOutputPathPrefix);
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

        conf.set("mapred.job.queue.name", "root.mis.mis_mr");
        conf.set("mapred.reduce.slowstart.completed.maps", "0.8");
        conf.set("mapred.child.java.opts", "-Xmx4096m");
        conf.set("mapred.output.compress", "true");
        conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("mapred.job.priority", "VERY_HIGH");
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.reduce.memory.mb", "4096");

        Job job = new Job(conf, "GenCalEvent@" + partitionDate);
        job.setJarByClass(GenCalEventMR.class);

        MultipleOutputs.addNamedOutput(job,GenCalEventMR.PERF_MOS, SequenceFileAsBinaryOutputFormat.class,Text.class,Text.class);
        job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
        job.setMapperClass(GenCalEventMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(GenCalEventReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(10);

        String[] inputArray = inputs.split(",");

        for (String inputPath : inputArray) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
