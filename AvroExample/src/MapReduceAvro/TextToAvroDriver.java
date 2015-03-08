package MapReduceAvro;

import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import entitiesAvro.MonitorDatos;

public class TextToAvroDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		JobConf jobConf = new JobConf(conf);

		Job job = Job.getInstance(conf);
		job.setJarByClass(TextToAvroDriver.class);

		// TODO: specify input and output DIRECTORIES
		FileInputFormat.setInputPaths(job, new Path(
				"/home/pruebahadoop/Documentos/DataSets/monitores/input/monitores3.csv"));
		FileOutputFormat.setOutputPath(job, new Path("/home/pruebahadoop/Documentos/DataSets/monitores/outputAvro"));

		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(TextToAvroMapper.class);
		// job.setMapOutputKeyClass(MonitorDatos.class);
		// job.setMapOutputValueClass(AvroKeyValueOutputFormat.class);

		// job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		// job.setReducerClass(TextToAvroReducer.class);
		// org.apache.avro.mapred.AvroJob.setReducerClass(jobConf,
		// TextToAvroReducer.class);
		// AvroJob.setInputKeySchema(job, MonitorDatos.getClassSchema());
		// AvroJob.setOutputKeySchema(job, MonitorDatos.getClassSchema());
		// AvroJob.setOutputValueSchema(job, MonitorDatos.getClassSchema());

		// job.setMapOutputKeyClass(Text.class);
		// AvroJob.setMapOutputValueSchema(job, MonitorDatos.getClassSchema());

		// Schema schema = MonitorDatos.getClassSchema();
		Schema schema = new Schema.Parser().parse(new File("schemas/monitor.avsc"));
		AvroJob.setReducerClass(jobConf, TextToAvroReducer.class);
		AvroJob.setOutputSchema(jobConf, schema);
		// Schema stringSchema = Schema.create(Schema.Type.STRING);
		Schema pairSchema = MonitorDatos.getClassSchema();
		AvroJob.setMapOutputSchema(jobConf, pairSchema);
		AvroJob.setInputSchema(jobConf, MonitorDatos.getClassSchema());

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
