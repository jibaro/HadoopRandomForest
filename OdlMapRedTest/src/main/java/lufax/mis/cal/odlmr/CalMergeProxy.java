package lufax.mis.cal.odlmr;

import lufax.mis.cal.odlmr.action.GenUserLinkingAction;
import lufax.mis.cal.utils.ProcessShell;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by geyanhao801 on 7/22/15.
 */
public class CalMergeProxy {
    public static boolean createRunningFlag(){
        try{
            String shell = "hadoop fs -touchz /user/mis/etl/_RUNNING";

            ProcessShell process = new ProcessShell();
            process.processShellWithoutResult(shell);

            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }


    public static void main(String[] args) throws Exception{
        String partitionDate = args[2];
        ApplicationContext contextAction = new ClassPathXmlApplicationContext(new String[]{"application.xml"});
        GenUserLinkingAction genUserLinkingAction = (GenUserLinkingAction)contextAction.getBean("genUserLinkingAction");
        genUserLinkingAction.genUserLinking(partitionDate);



    }


}
