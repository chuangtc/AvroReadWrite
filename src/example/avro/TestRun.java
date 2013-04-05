package example.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.*;
import org.apache.avro.io.*;
import org.apache.avro.specific.*;

public class TestRun {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		User user1 = new User();
		user1.setUserId(1);
		user1.setName("Alyssa");
		user1.setFavoriteNumber(256);
		// Leave favorite color null
		
		// Alternate constructor
		User user2 = new User(2,"Ben", 7, "red");

		// Construct via builder
		User user3 = User.newBuilder()
					 .setUserId(3)
		             .setName("Charlie")
		             .setFavoriteColor("blue")
		             .setFavoriteNumber(null)
		             .build();
		File file = new File("users.avro");
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
		dataFileWriter.create(user1.getSchema(), new File("users.avro"));
		dataFileWriter.append(user1);
		dataFileWriter.append(user2);
		dataFileWriter.append(user3);
		dataFileWriter.close();
		
		
		// Deserialize Users from disk
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
		DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader);
		User user = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			user=dataFileReader.next();
			System.out.println(user);
		}
	}

}
