package MapReduceAvro;

import java.io.IOException;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.mapred.Reporter;

import entitiesAvro.MonitorDatos;

public class TextToAvroReducer extends AvroReducer<Utf8, Utf8, MonitorDatos>{

	public void reduce(MonitorDatos arg0, Iterable<MonitorDatos> arg1, AvroCollector<MonitorDatos> arg2, Reporter arg3)
			throws IOException {
		for (MonitorDatos md : arg1) {
			arg2.collect(md);
		}
	}
}
