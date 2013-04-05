package example.avro;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//It's writing to HDFS file as TextFile format

public class HDFSTestRunGeneric {

	public static FileSystem hdfs;
	
	public static void main(String[] args)  {
				
		if(args.length!=1){
			System.out.println("Please provide properties file. Then run it as follows.");
			System.out.println("HDFSTestRunGeneric prj.properties");
			System.exit(1);
		}
		
		//build properties object from file parameter		
		String property_file_path = args[0];
		Properties prop_config = new Properties();
		try {
			BufferedReader br = new BufferedReader(new FileReader(property_file_path));
			prop_config.load(br);
			br.close();
			//build hadoop configuration		
			String[] hadoop_config_resources = prop_config.getProperty("hadoop_config_resource_list").split(",");
			Configuration hadoop_config = new Configuration();
			for (String config_resource : hadoop_config_resources) {
				hadoop_config.addResource(new Path(config_resource));
			}
			hdfs = FileSystem.get(hadoop_config);
		}
		catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		
		Path avroFileWrite = new Path(prop_config.getProperty("avro_hadoop_file_path_write"));
		
		try{
			writeData(avroFileWrite,prop_config.getProperty("avro_schema_file_path"));
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		Path avroFileRead = new Path(prop_config.getProperty("avro_hadoop_file_path_read"));
		readData(avroFileRead);
		
		
	}
	
	
	public static void writeData(Path fpath, String schemaFileName) throws Exception{
		Schema s;
		s = new Schema.Parser().parse(new File(schemaFileName));
		           
        GenericDatumWriter<GenericRecord> datum = new GenericDatumWriter<GenericRecord>(s);            
        DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(datum);
        
        writer.create(s,new BufferedOutputStream(hdfs.create(fpath)));
		
        String fieldName[] = new String[s.getFields().size()];
        for(Field field:s.getFields()){
        	fieldName[field.pos()]=s.getFields().get(field.pos()).name();
        }
        
        GenericData.Record record = new GenericData.Record(s);
        record.put(fieldName[0],1);
        record.put(fieldName[1],"hello world 1");
        record.put(fieldName[2], 111);
        writer.append(record);
        record = new GenericData.Record(s);
        record.put(fieldName[0],2);
        //record.put(fieldName[1],"hello world 2");
        record.put(fieldName[2], 222);
        writer.append(record);
        record = new GenericData.Record(s);
        record.put(fieldName[0],3);
        record.put(fieldName[1],"hello world 3");
        record.put(fieldName[2], 333);
        writer.append(record);
        record = new GenericData.Record(s);
        record.put(fieldName[0],4);
        record.put(fieldName[1],"hello world 4");
        record.put(fieldName[2], 444);
        writer.append(record);
        record = new GenericData.Record(s);
        record.put(fieldName[0],5);
        record.put(fieldName[1],"hello world 5");
        //record.put(fieldName[2], 555);
        writer.append(record);
        
        writer.close();
	}
	
	public static void readData(Path path){
		GenericDatumReader<GenericRecord> datum = new GenericDatumReader<GenericRecord>();
	    
		try {
			DataFileStream<GenericRecord> importReader;
			importReader = new DataFileStream<GenericRecord>(new BufferedInputStream(hdfs.open(path)), datum);
			Schema schema = importReader.getSchema();
			
			System.out.println(schema);
			
			int max=10,i=0;
			//read until there are no more files and the import reader is null
			while (importReader.hasNext() && i<max) {

				GenericRecord record=importReader.next();
				System.out.println(record);
				i++;
			}
			importReader.close();

		
		} 
		catch (IOException e) {
			e.printStackTrace();
		}						
		
		
	}
}
