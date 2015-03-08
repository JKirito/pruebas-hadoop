import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Latitud_MinMaxDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(Latitud_MinMaxDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(Latitud_MinMaxMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(Latitud_MinMaxReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(NullWritable.class);

		// TODO: specify input and output DIRECTORIES
		FileInputFormat.setInputPaths(job, new Path(
				"/home/pruebahadoop/Documentos/DataSets/monitores/input/monitores2.csv"));
		FileOutputFormat.setOutputPath(job, new Path("/home/pruebahadoop/Documentos/DataSets/monitores/output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
