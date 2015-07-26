package lufax.mis.cal.utils;

import lufax.mis.cal.common.record.TSessionRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by lufax on 7/6/15.
 */
public class TSessionUtils {
    public static final Map<String, String> tSessionMap = new HashMap<String, String>();

    //现阶段只提供连续时间段的session
    public static boolean initTSessionMap(Configuration conf, String pathPrefix, Date endDate, int interval, int unit) throws IOException {

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(endDate);

        Set<String> partitions = new HashSet<String>();
        for (int i = 0; i < interval; i++) {
            calendar.add(unit, -i);
            String partition = "rd=" + DateUtils.getRd(calendar.getTime());
            partitions.add(partition);
            calendar.add(unit, i);
        }

        tSessionMap.clear();
        for (String partition : partitions) {
            String sessionPath = pathPrefix + partition;
            System.out.println("Session path is " + sessionPath);

            Path path = new Path(sessionPath);
            FileSystem fs = FileSystem.get(conf);

            if (!fs.exists(path)) {
                continue;
            }

            if (fs.isDirectory(path)) {
                FileStatus[] fileStatuses = fs.listStatus(path);
                for (FileStatus fileStatus : fileStatuses) {
                    Path childPath = fileStatus.getPath();
                    //只处理文件，不会递归处理目录
                    if (fs.isFile(childPath)) {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(childPath)));

                        String line;
                        line = reader.readLine();
                        while (line != null){
                            TSessionRecord record = new TSessionRecord();
                            record.setRawValue(line);

                            System.out.println(record.getSessionID() + " <---> " + record.getUserID());
                            tSessionMap.put(record.getSessionID(), record.getUserID());

                            line = reader.readLine();
                        }
                        reader.close();
                    }
                }

            } else if (fs.isFile(path)) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                String line;
                line = reader.readLine();
                while (line != null){
                    TSessionRecord record = new TSessionRecord();
                    record.setRawValue(line);

                    tSessionMap.put(record.getId(), record.getUserID());
                }
                reader.close();
            }


        }
        return true;
    }

    public static String getUserIDBySession(String sessionID) {
        if (tSessionMap.containsKey(sessionID)) {
            return tSessionMap.get(sessionID);
        }

        return "";
    }
}
