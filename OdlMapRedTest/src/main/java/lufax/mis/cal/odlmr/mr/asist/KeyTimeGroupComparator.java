package lufax.mis.cal.odlmr.mr.asist;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by geyanhao801 on 7/16/15.
 */
public class KeyTimeGroupComparator extends WritableComparator {
    protected KeyTimeGroupComparator() {
        super(KeyTimePair.class, true);
    }

    @Override
    public int compare(WritableComparable w1,WritableComparable w2){
        KeyTimePair pair1 = (KeyTimePair) w1;
        KeyTimePair pair2 = (KeyTimePair) w2;
        return pair1.getKey().compareTo(pair2.getKey());

    }

}
