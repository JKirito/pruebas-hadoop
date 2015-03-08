package MapReduceAvro;
import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import entitiesAvro.Monitor.MonitorCampo;
import entitiesAvro.MonitorDatos;

public class TextToAvroMapper extends Mapper<LongWritable, Text, AvroKey<MonitorDatos>, AvroValue<Utf8>> {
	private static final String SEPARATOR_SYMBOL = ",";

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		System.out.println("AAAAAAAAAAAAAAA");

		final String[] values = value.toString().split(SEPARATOR_SYMBOL);
		//Si es el header, lo salteo
		if (values[entitiesAvro.Monitor.MonitorCampo.FNAME.value()].equals(entitiesAvro.Monitor.MonitorCampo.FNAME.name())) {
			return;
		}

		MonitorDatos monitorRecord = new MonitorDatos();
		monitorRecord.setFNAME(values[MonitorCampo.FNAME.value()]);
		monitorRecord.setLONGITUD(Double.parseDouble(values[MonitorCampo.LONGITUD.value()]));
		monitorRecord.setLATITUD(Double.parseDouble(values[MonitorCampo.LATITUD.value()]));

		context.write(new AvroKey<MonitorDatos>(monitorRecord), new AvroValue<Utf8>());
	};
}
