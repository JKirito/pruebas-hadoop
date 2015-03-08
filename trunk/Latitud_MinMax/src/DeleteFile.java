import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class DeleteFile {

	public DeleteFile() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws IOException {

		String dst = "hdfs://localhost/";
		String pathABorrar = "/ejemplo.tgz";

		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		/**
		 * si el parametro es true, borra recursivamente (si es directorio, si no se ignora)
		 */
		System.out.println(fs.delete(new Path(pathABorrar), false));
	}

}
