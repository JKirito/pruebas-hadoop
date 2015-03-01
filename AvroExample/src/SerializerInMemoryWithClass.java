import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class SerializerInMemoryWithClass {

	private static List<StringPair> list = CreateObjects.CreateThreeExampleStringPair();

	/**
	 * We create a DatumWriter, which converts Java objects into an in-memory
	 * serialized format. The SpecificDatumWriter class is used with generated
	 * classes and extracts the schema from the specified generated type.
	 *
	 * Next we create a DataFileWriter, which writes the serialized records, as
	 * well as the schema, to the file specified in the dataFileWriter.create
	 * call. We write our objects to the file via calls to the
	 * dataFileWriter.append method. When we are done writing, we close the data
	 * file.
	 *
	 * @throws IOException
	 */
	public static void Serialize() throws IOException {
		System.out.println("Comienzo Serializacion...");
		// Serialize user1, user2 and user3 to disk
		DatumWriter<StringPair> userDatumWriter = new SpecificDatumWriter<StringPair>(StringPair.class);
		DataFileWriter<StringPair> dataFileWriter = new DataFileWriter<StringPair>(userDatumWriter);
		dataFileWriter.create(list.get(0).getSchema(), new File("ejemploStringPair.avro"));
		dataFileWriter.append(list.get(0));
		dataFileWriter.append(list.get(1));
		dataFileWriter.append(list.get(2));
		dataFileWriter.close();
		System.out.println("Fin Serializacion.");
		System.out.println();
	}

	/**
	 * Deserializing is very similar to serializing. We create a
	 * SpecificDatumReader, analogous to the SpecificDatumWriter we used in
	 * serialization, which converts in-memory serialized items into instances
	 * of our generated class. We pass the DatumReader and the previously
	 * created File to a DataFileReader, analogous to the DataFileWriter, which
	 * reads the data file on disk.
	 *
	 * Next we use the DataFileReader to iterate through the serialized Users
	 * and print the deserialized object to stdout. Note how we perform the
	 * iteration: we create a single (StringPair) object which we store the
	 * current deserialized (stringPair) in, and pass this record object to
	 * every call of dataFileReader.next. This is a performance optimization
	 * that allows the DataFileReader to reuse the same (StringPair) object
	 * rather than allocating a new (StringPair) for every iteration, which can
	 * be very expensive in terms of object allocation and garbage collection if
	 * we deserialize a large data file. While this technique is the standard
	 * way to iterate through a data file, it's also possible to use for
	 * (StringPair stringPair : dataFileReader) if performance is not a concern.
	 *
	 * @throws IOException
	 */
	public static void Deserialize() throws IOException {
		System.out.println("Comienzo Deserializacion...");
		// Deserialize Users from disk
		DatumReader<StringPair> userDatumReader = new SpecificDatumReader<StringPair>(StringPair.class);
		File file = new File("ejemploStringPair.avro");
		DataFileReader<StringPair> dataFileReader = new DataFileReader<StringPair>(file, userDatumReader);
		StringPair stringPair = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			stringPair = dataFileReader.next(stringPair);
			System.out.println(stringPair);
		}
		dataFileReader.close();
		System.out.println("Fin Deserializacion.");
		System.out.println();
	}

	public static void main(String[] args) throws IOException {
		Serialize();
		Deserialize();
	}

}
