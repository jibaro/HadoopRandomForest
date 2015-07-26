package lufax.mis.cal.odlmr.mr.asist;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by geyanhao801 on 7/16/15.
 */
public class KeyTimePartitioner extends Partitioner<KeyTimePair,Text> {
    @Override
    public int getPartition(KeyTimePair keyTimePair,Text value,int numPartition){
        return -Math.abs(keyTimePair.getKey().toString().hashCode()) % numPartition;
    }
}
