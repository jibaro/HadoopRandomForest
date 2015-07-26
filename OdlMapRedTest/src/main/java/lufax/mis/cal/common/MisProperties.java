package lufax.mis.cal.common;

import java.io.*;
import java.util.Properties;

public class MisProperties {
//	private String filePath;
	private Properties props = new Properties();

	public boolean init(String filePath) {
		try {
			InputStream input = new BufferedInputStream(new FileInputStream(filePath));
			props.load(input);
			return true;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public Properties getProps() {
		return props;
	}

	public void setProps(Properties props) {
		this.props = props;
	}

	public static MisProperties getMisProperties(String filePath) {
		MisProperties props = new MisProperties();
		props.init(filePath);
		return props;
	}
}
