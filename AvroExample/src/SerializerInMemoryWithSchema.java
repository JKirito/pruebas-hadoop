import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 * In-Memory Serialization and Deserialization
 *
 * @author pruebahadoop
 *
 */
public class SerializerInMemoryWithSchema {

	public static void main(String args[]) throws IOException {
		SerializerInMemoryWithSchema lf = new SerializerInMemoryWithSchema();
		lf.abc();
	}

	public void abc() throws IOException {
		System.out.println("inicio");

		/**
		 * Para general el código automáticamente a partir del schema, ejecutar
		 * java -jar /path/to/avro-tools-1.7.7.jar compile schema <schema file>
		 * <destination>
		 */

		// First, we use a Parser to read our schema definition and create a
		// Schema object.
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(new File("schemas/StringPair.avsc"));

		// We can create an instance of an Avro record using the generic API as
		// follows:
		GenericRecord datumStringPair = new GenericData.Record(schema);
		datumStringPair.put("left", "L");
		datumStringPair.put("right", "R");

		// Since we're not using code generation, we use GenericRecords to
		// represent users. GenericRecord uses the schema to verify that we only
		// specify valid fields. If we try to set a non-existent field, we'll
		// get an AvroRuntimeException when we run the program.

		// Serializo
		// Next, we serialize the record to an output stream:
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		writer.write(datumStringPair, encoder);
		encoder.flush();
		out.close();

		// Si quisiera serializar a disco...
		// First we'll serialize our objects to a data file on disk.
		/**
		 * We create a DatumWriter, which converts Java objects into an
		 * in-memory serialized format. Since we are not using code generation,
		 * we create a GenericDatumWriter. It requires the schema both to
		 * determine how to write the GenericRecords and to verify that all
		 * non-nullable fields are present.
		 *
		 * As in the code generation example, we also create a DataFileWriter,
		 * which writes the serialized records, as well as the schema, to the
		 * file specified in the dataFileWriter.create call. We write our users
		 * to the file via calls to the dataFileWriter.append method. When we
		 * are done writing, we close the data file.
		 */
		// File file = new File("pairsString.avro");
		// DatumWriter<GenericRecord> datumWriter = new
		// GenericDatumWriter<GenericRecord>(schema);
		// DataFileWriter<GenericRecord> dataFileWriter = new
		// DataFileWriter<GenericRecord>(datumWriter);
		// dataFileWriter.create(schema, file);
		// dataFileWriter.append(datumStringPair);
		// dataFileWriter.close();

		// Deserializo
		// We can reverse the process and read the object back from the byte
		// buffer:
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
		Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		GenericRecord result = reader.read(null, decoder);
		System.out.println(result.get("left").toString());
		System.out.println(result.get("right").toString());


		//Finally, we'll deserialize the data file we just created.
		// Deserialize users from disk
		// DatumReader<GenericRecord> datumReader = new
		// GenericDatumReader<GenericRecord>(schema);
		// File file = new File("pairsString.avro");
		// DataFileReader<GenericRecord> dataFileReader = new
		// DataFileReader<GenericRecord>(file, datumReader);
		// GenericRecord user = null;
		// while (dataFileReader.hasNext()) {
		// // Reuse user object by passing it to next(). This saves us from
		// // allocating and garbage collecting many objects for files with
		// // many items.
		// user = dataFileReader.next(user);
		// System.out.println(user);

	}
}
