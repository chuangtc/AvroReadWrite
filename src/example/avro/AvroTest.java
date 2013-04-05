package example.avro;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import org.apache.log4j.Logger;

/*
 * hadoop jar dist/avroTest.jar example.avro.AvroTest prj.properties
 * 
 */

public class AvroTest {
	
	private static final Logger logger = Logger.getLogger(AvroTest.class);	

	public static void main(String[] args){
		
		String property_file_path = "prj.properties";
		
		logger.info("Using property file : "+property_file_path);
		System.out.println("Copyright 2013 Tzu-Cheng Chuang");

		if (args.length != 1) {
			logger.error("Usage: java -jar avroTest.jar <command>");
			System.out.println("-----------------");
			System.out.println("Available commands:");
			System.out.println("demo1  Use Avro Genrated Java class to write/read to/from local file system.");			
			System.out.println("demo2  Use Avro GenericRecord to write/read to/from local file system.");
			System.out.println("demo3  Use Avro GenericRecrod to write/read to/from HDFS in TextFile foramt");
			
			System.exit(1);			
		}
		//build properties object from file parameter
		Properties prop_config = new Properties();
		try {
			BufferedReader br = new BufferedReader(new FileReader(property_file_path));
			prop_config.load(br);
			br.close();
		}
		catch(Exception e) {
			logger.error("Unable to load program properties: " + e.getMessage());
			System.exit(1);
		}
		
		String avro_schema_file_path = prop_config.getProperty("avro_schema_file_path");
		String avro_hadoop_config_file_path = prop_config.getProperty("hadoop_config_resource_list");
		
		logger.info("Avro schema : "+avro_schema_file_path);
		logger.info("Hadoop config file path : "+avro_hadoop_config_file_path);
		
		try{
			if(args[0].compareTo("demo1")==0){
				logger.info("demo1");
				TestRun.main(null);
			}
			else if(args[0].compareTo("demo2")==0){
				logger.info("demo2");
				TestRunGeneric.main(null);
			} 
			else if(args[0].compareTo("demo3")==0){
				logger.info("demo3");
				HDFSTestRunGeneric.main(new String[]{property_file_path});
			}

		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		
	}
	
}
