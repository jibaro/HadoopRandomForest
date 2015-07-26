package lufax.mis.cal.utils;

import java.util.Date;

/**
 * Created by lufax on 6/29/15.
 */
public class HdfsUtils {
    public static boolean deleteOutputFilePath(String path) {
        System.out.print(new Date());
        System.out.println("Delete output file path " + path);
        ProcessShell process = new ProcessShell();
        try {
            String lsCmd = "hadoop fs -ls " + path;
            process.processShellWithoutResult(lsCmd);
        } catch (RuntimeException e) {
            //说明这个目录不存在
//			System.out.print(new Date());
            LogUtils.log2Screen("Target directory [" + path + "] not existed");
            return true;
        }

        try {
            String lsCmd = "hadoop fs -rmr " + path;
            process.processShellWithoutResult(lsCmd);
        } catch (RuntimeException e) {
            //说明这个目录不存在
//			System.out.print(new Date());
            LogUtils.log2Screen("Delete target directory failed");
            return false;
        }
        return true;
    }
}
