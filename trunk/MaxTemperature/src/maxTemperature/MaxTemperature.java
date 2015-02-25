package maxTemperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(maxTemperature.MaxTemperature.class);
		// specify a mapper
		job.setMapperClass(MaxTemperatureMapper.class);
		// specify a reducer
		job.setReducerClass(MaxTemperatureReducer.class);

		// specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/home/pruebahadoop/Documentos/DataSets/clima/input/"));
		FileOutputFormat.setOutputPath(job, new Path("/home/pruebahadoop/Documentos/DataSets/clima/output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
