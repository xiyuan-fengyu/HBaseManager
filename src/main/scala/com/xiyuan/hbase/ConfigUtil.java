package com.xiyuan.hbase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ConfigUtil {

	private static final String PACKAGE_ROOT = "./src/main/scala/";
	
	public static void main(String[] args) {
		propertiesToClass("HBaseConfig.properties", "com.xiyuan.hbase");
	}
	
	private static final String rLongOrInt = "-[0-9]{1,19}|[+]{0,1}[0-9]{1,19}";
	private static final String rDouble = "[-+]{0,1}[0-9]+\\.[0-9]+";
	private static final String rBoolean = "true|false";
	
	private static void propertiesToClass(String fileName, String packageStr) {
		Properties properties = new Properties();
		try {
			properties.load(ConfigUtil.class.getClassLoader().getResourceAsStream(fileName));
			Set<Map.Entry<Object, Object>> keyVals = properties.entrySet();
			
			StringBuffer strBF = new StringBuffer();
			for (Map.Entry<Object, Object> entry : keyVals) {
				String key = (String) entry.getKey();
				String val = (String) entry.getValue();
				String keyInJava = key.replaceAll("\\.", "_");
				if(val.matches(rBoolean)) {
					strBF.append("\tpublic static final boolean " + keyInJava + " = Boolean.parseBoolean(properties.getProperty(\"" + key + "\"));\n\n");
				}
				else if(val.matches(rDouble)) {
					strBF.append("\tpublic static final double " + keyInJava + " = Double.parseDouble(properties.getProperty(\"" + key + "\"));\n\n");
				}
				else if(val.matches(rLongOrInt)) {
					long tempL = Long.parseLong(val);
					if(tempL >= Integer.MIN_VALUE && tempL <= Integer.MAX_VALUE) {
						strBF.append("\tpublic static final int " + keyInJava + " = Integer.parseInt(properties.getProperty(\"" + key + "\"));\n\n");
					}
					else if(tempL >= Long.MIN_VALUE && tempL <= Long.MAX_VALUE) {
						strBF.append("\tpublic static final long " + keyInJava + " = Long.parseLong(properties.getProperty(\"" + key + "\"));\n\n");
					}
					else {
						strBF.append("\tpublic static final String " + keyInJava + " = properties.getProperty(\"" + key + "\");\n\n");
					}
				}
				else {
					strBF.append("\tpublic static final String " + keyInJava + " = properties.getProperty(\"" + key + "\");\n\n");
				}
			}
			
			String className = fileName.split("\\.")[0];
			char[] cArr = className.toCharArray();
			if (cArr.length > 0 && cArr[0] >= 'a' && cArr[0] <= 'z') {
				cArr[0] = (char)(cArr[0] + (int)'A' - (int)'a');
				className = new String(cArr);
			}
			
			String classStr = "package " + packageStr + ";\n\n" +
					"import java.util.Properties;\n" +
					"import " + ConfigUtil.class.getPackage().getName() + ".ConfigUtil;\n\n" +
					"public class " + className + " {\n\n" +
						"\tprivate static final Properties properties = ConfigUtil.loadProperties(\"" + fileName + "\");\n\n" +
						strBF.toString() +
					"}";
			File dir = new File(PACKAGE_ROOT + packageStr.replaceAll("\\.", "/"));
			if (!dir.exists()) {
				dir.mkdirs();
			}
			File classFile = new File(PACKAGE_ROOT + packageStr.replaceAll("\\.", "/") + "/" + className + ".java");
			if(!classFile.exists()) {
				classFile.createNewFile();
			}
			FileOutputStream out = new FileOutputStream(classFile);
			out.write(classStr.getBytes("UTF-8"));
			out.flush();
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Properties loadProperties(String fileName) {
		Properties properties = new Properties();
		try {
			properties.load(ConfigUtil.class.getClassLoader().getResourceAsStream(fileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return properties;
	}
}