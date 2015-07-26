package lufax.mis.cal.odlmr.mr.asist;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by geyanhao801 on 7/16/15.
 */
public class KeyTimePair implements WritableComparable<KeyTimePair>{
    private Text key = new Text();
    private LongWritable timestamp = new LongWritable();

    public KeyTimePair() {

    }
    public KeyTimePair(Text key,LongWritable timestamp) {
        this.key = key;
        this.timestamp = timestamp;
    }

    public Text getKey(){
        return key;
    }
    public void setKey(String key){
        this.key = new Text(key);
    }

    public LongWritable getTimestamp(){
        return timestamp;
    }
    public void setTimestamp(Long timestamp){
        this.timestamp = new LongWritable(timestamp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        timestamp.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        timestamp.write(out);
    }

    @Override
    public int compareTo(KeyTimePair other) {
        int cmp = this.key.compareTo(other.key);

        if (cmp == 0){
            cmp = -this.timestamp.compareTo(other.timestamp);
        }

        return cmp;
    }

    @Override
    public String toString(){
        String outKey = String.format("%s_%s",key.toString(),timestamp.toString());
        return outKey;
    }

    public static void main(String[] args){
        String totalKey = "5828d83ff10549d3743c26626a2224ed:1435358549187";
        String[] cols = totalKey.split(":");
        Text key = new Text(cols[0]);
        LongWritable timestamp = new LongWritable(Long.parseLong(cols[1]));
        KeyTimePair ktp = new KeyTimePair(key,timestamp);
        System.out.println(ktp.toString());



    }


}
