package lufax.mis.cal.common;

public class MisCommon {
	//用来确认CalStreaming的运行状态
	public static final int INIT = 0;
	public static final int RUNNING = 1;
	public static final int MERGING = 2;
	public static final int DONE = 3;
	public static final int FAILED = 4;

    //用来确认网址的类型
    public static final int CAL = 1;
    public static final int PERF = 2;
    public static final int OTHER = 3;
}
