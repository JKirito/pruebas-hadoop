import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class ReadFile {

	public ReadFile() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws IOException {
		String uri = "/Página_12/Notas/Texto/Pagina12_2014-12-15_Respaldo para la AFIP.txt";
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(URI.create("hdfs://localhost"), conf);

		InputStream in = null;
		try {

			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}

	}

}
