import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class ReadFileMain {

	public ReadFileMain() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws IOException {
		ReadFileHDFS a = new ReadFileHDFS("/Página_12/Notas/Texto/Pagina12_2014-12-15_Respaldo para la AFIP.txt");
		InputStream s = a.getStream();
		StringSetCreator b = new StringSetCreator();
		System.out.println(b.getStringSet(s, "iso-8859-1"));

		if (true) {
			return;
		}
		String uri = "/Página_12/Notas/Texto/Pagina12_2014-12-15_Respaldo para la AFIP.txt";
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(URI.create("hdfs://localhost"), conf);

		InputStream in = null;
		try {

			in = fs.open(new Path(uri));
			// Salida por pantalla
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}

	}

}
