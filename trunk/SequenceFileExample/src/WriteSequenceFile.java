import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class WriteSequenceFile {

	private static final String rutaDestino = new String("/home/pruebahadoop/Documentos/sequencefileEjemplo");
	private static final String rutaOrigen = new String(
			"/home/pruebahadoop/Documentos/DescargasPeriodicos/Procesado/LaNacion/PRUEBA");

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(rutaDestino);
		IntWritable key = new IntWritable();
		Text value = new Text();

		// Creamos el writer del SequenceFile para poder ir añadiendo
		// los pares key/value al fichero.
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, key.getClass(), value.getClass());
		File dirNotas = new File(rutaOrigen);
		for (File f : dirNotas.listFiles()) {
			// La key es el número de línea
			key.set(f.getName().hashCode());
			ReaderFile fileRead = new ReaderFile(f);
			// El value es la línea del poema correspondiente
			value.set(fileRead.leer());
			// Escribimos el par en el sequenceFile
			writer.append(key, value);
		}

		writer.close();
	}

}
