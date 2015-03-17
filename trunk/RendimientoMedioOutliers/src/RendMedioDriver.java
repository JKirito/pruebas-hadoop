import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import entities.CeldaWritable;

public class RendMedioDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");

		System.setProperty("longitudMin", "-62.3161004401404");
		System.setProperty("latitudMin", "-37.5108465428739");
		System.setProperty("anchoCelda", "0.0001");

		job.setJarByClass(RendMedioDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(RendMedioMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(RendMedioReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(CeldaWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		// TODO: specify input and output DIRECTORIES
		FileInputFormat.setInputPaths(job, new Path(
				"/home/pruebahadoop/Documentos/DataSets/monitores/input/monitores2.csv"));
		FileOutputFormat.setOutputPath(job, new Path("/home/pruebahadoop/Documentos/DataSets/monitores/outputSinOutliers"));

		if (!job.waitForCompletion(true))
			return;
	}

}
