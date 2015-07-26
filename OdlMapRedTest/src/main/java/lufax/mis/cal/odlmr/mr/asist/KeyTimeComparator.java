package lufax.mis.cal.odlmr.mr.asist;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by geyanhao801 on 7/16/15.
 */
public class KeyTimeComparator extends WritableComparator {
    protected KeyTimeComparator() {super(KeyTimePair.class,true);}

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        KeyTimePair keyTimePair1 = (KeyTimePair) w1;
        KeyTimePair keyTimePair2 = (KeyTimePair) w2;

        //前一部分的key正向排序
        if (!keyTimePair1.getKey().equals(keyTimePair2.getKey())) {
            return keyTimePair1.getKey().compareTo(keyTimePair2.getKey());
        }

        //后一部分的timestamp倒序排序
        int timestampCmp = -keyTimePair1.getTimestamp().compareTo(keyTimePair2.getTimestamp());
        return timestampCmp;
    }

}
