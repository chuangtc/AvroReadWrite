package example.avro;


import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

 
public class TestRunGeneric {
 
       public static void main(String args[]) throws Exception {
    	   String schemaFileName="tableexample.avsc";
    	   
           writeData("tableexample_out.txt",schemaFileName);
           readData("tableexample_out.txt");
       }
      
       private static void writeData(String fpath,String schemaFileName) throws Exception {
           
    	   Schema s = new Schema.Parser().parse(new File(schemaFileName));           
           GenericDatumWriter<GenericRecord> datum = new GenericDatumWriter<GenericRecord>(s);            
           DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(datum);
           
           writer.create(s,new File(fpath));
           GenericData.Record record = new GenericData.Record(s);
           record.put("rowid",1);
           record.put("name","hello world 1");
           record.put("number", 111);
           writer.append(record);
           record = new GenericData.Record(s);
           record.put("rowid",2);
           //record.put("name","hello world 2");
           record.put("number", 222);
           writer.append(record);
           record = new GenericData.Record(s);
           record.put("rowid",3);
           record.put("name","hello world 3");
           record.put("number", 333);
           writer.append(record);
           writer.close();
 
       }
      
       private static void readData(String fpath) throws Exception {
    	   File f = new File(fpath);
    	   GenericDatumReader<GenericRecord> datum = new GenericDatumReader<GenericRecord>();
    	   DataFileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(f, datum);
    	   Schema s = reader.getSchema();
    	   GenericData.Record record = new GenericData.Record(s);
    	   int cnt=0;
    	   while (reader.hasNext()) {
    		   reader.next(record);
    		   cnt++;
    		   
    		   for (Schema.Field field : s.getFields()) {
    			   System.out.println("name=" + field.name() + ". value=" + record.get(field.pos()) + ".");
    		   }
    		   
    	   }
    	   System.out.println("total count:"+cnt);

    	   reader.close();
       }
}