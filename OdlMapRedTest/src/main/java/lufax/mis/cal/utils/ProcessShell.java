package lufax.mis.cal.utils;

import com.google.common.base.Throwables;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class ProcessShell {
//	protected Logger logger = null;

    class InfoStreamDrainer implements Runnable {

        private InputStream ins;

        public InfoStreamDrainer(InputStream ins) {
            this.ins = ins;
        }

        public void run() {
            try {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(ins));
                String line = null;
                while ((line = reader.readLine()) != null) {
//                    logger.info(line);
                	System.out.println(line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    class ErrorStreamDrainer implements Runnable {

        private InputStream ins;

        public ErrorStreamDrainer(InputStream ins) {
            this.ins = ins;
        }

        public void run() {
            try {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(ins));
                String line = null;
                while ((line = reader.readLine()) != null) {
//                    logger.error(line);
                	System.err.print(line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

	public void processShellWithoutResult(String[] processCmd) throws RuntimeException {
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(processCmd);
            new Thread(new ErrorStreamDrainer(process.getErrorStream())).start();
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String output = null;
            while ((output = br.readLine()) != null) {
//            	logger.info(output);
            	System.out.println(output);
            }

            int rs = process.waitFor();

            if (0 != rs) {
                if (9 == rs || 10 == rs) {
                	LogUtils.log2Screen("sorry! first exec error!! waiting for 90 seconds to continue exec cmd, please be patient!!");
                    TimeUnit.SECONDS.sleep(90);

                    process = Runtime.getRuntime().exec(processCmd);

                    br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    output = null;
                    while ((output = br.readLine()) != null) {
                    	System.out.println(output);
                    }

                    rs = process.waitFor();

                    if (0 != rs) {
                        throw new RuntimeException("Process exe Error:" + process.exitValue());
                    }
                } else {
                    throw new RuntimeException("Process exe Error:" + process.exitValue());
                }
            }


        } catch (Throwable e) {
//            logger.error(Throwables.getStackTraceAsString(e));
            System.err.println(Throwables.getStackTraceAsString(e));

            throw new RuntimeException(e);

        } finally {
            if (null != process) {
                process.destroy();
            }
        }
        return;
    }

	public void processShellWithoutResult(String processCmd) throws RuntimeException {
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(processCmd);
            new Thread(new ErrorStreamDrainer(process.getErrorStream())).start();
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String output = null;
            while ((output = br.readLine()) != null) {
//            	logger.info(output);
            	System.out.println(output);
            }

            int rs = process.waitFor();

            if (0 != rs) {
                if (9 == rs || 10 == rs) {
                	LogUtils.log2Screen("sorry! first exec error!! waiting for 90 seconds to continue exec cmd, please be patient!!");
                    TimeUnit.SECONDS.sleep(90);

                    process = Runtime.getRuntime().exec(processCmd);

                    br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    output = null;
                    while ((output = br.readLine()) != null) {
//                    	logger.info(output);
                    	System.out.println(output);
                    }

                    rs = process.waitFor();

                    if (0 != rs) {
                        throw new RuntimeException("Process exe Error:" + process.exitValue());
                    }
                } else {
                    throw new RuntimeException("Process exe Error:" + process.exitValue());
                }
            }


        } catch (Throwable e) {
        	System.err.println(Throwables.getStackTraceAsString(e));
            throw new RuntimeException(e);

        } finally {
            if (null != process) {
                process.destroy();
            }
        }
        return;
    }

    public List<String> processShellWithResult(String[] processCmd) throws RuntimeException {
        List<String> resultList = new ArrayList<String>();
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(processCmd);
            new Thread(new InfoStreamDrainer(process.getErrorStream())).start();
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String output = null;
            while ((output = br.readLine()) != null) {
                resultList.add(output);
            }

            int rs = process.waitFor();

            if (0 != rs) {
                if (9 == rs || 10 == rs) {
                	LogUtils.log2Screen("sorry! first exec error!! waiting for 90 seconds to continue exec cmd, please be patient!!");
                    TimeUnit.SECONDS.sleep(90);

                    resultList = null;
                    resultList = new ArrayList<String>();
                    process = Runtime.getRuntime().exec(processCmd);

                    br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    output = null;
                    while ((output = br.readLine()) != null) {
                        resultList.add(output);
                    }

                    rs = process.waitFor();

                    if (0 != rs) {
                        throw new RuntimeException("Process exe Error:" + process.exitValue());
                    }
                } else {
                    throw new RuntimeException("Process exe Error:" + process.exitValue());
                }
            }


        } catch (Throwable e) {
        	System.err.println(Throwables.getStackTraceAsString(e));
            throw new RuntimeException(e);

        } finally {
            if (null != process) {
                process.destroy();
            }
        }
        return resultList;
    }

    public String processShellWithHtmlResult(String[] processCmd) throws RuntimeException {
        StringBuffer sb = new StringBuffer();
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(processCmd);
            new Thread(new InfoStreamDrainer(process.getErrorStream())).start();
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String output = null;
            while ((output = br.readLine()) != null) {
            	sb.append(output).append("<br>");
            }

            int rs = process.waitFor();

            if (0 != rs) {
                if (9 == rs || 10 == rs) {
                	System.out.println("sorry! first exec error!! waiting for 90 seconds to continue exec cmd, please be patient!!");
                    TimeUnit.SECONDS.sleep(90);

                    sb = null;
                    sb = new StringBuffer();
                    process = Runtime.getRuntime().exec(processCmd);

                    br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    output = null;
                    while ((output = br.readLine()) != null) {
                        sb.append(output).append("<br>");
                    }

                    rs = process.waitFor();

                    if (0 != rs) {
                        throw new RuntimeException("Process exe Error:" + process.exitValue());
                    }
                } else {
                    throw new RuntimeException("Process exe Error:" + process.exitValue());
                }
            }


        } catch (Throwable e) {
        	System.err.println(Throwables.getStackTraceAsString(e));
            throw new RuntimeException(e);

        } finally {
            if (null != process) {
                process.destroy();
            }
        }
        return sb.toString();
    }

}
