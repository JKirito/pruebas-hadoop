import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Latitud_MinMaxMapper extends Mapper<LongWritable, Text, DoubleWritable, NullWritable> {

	private static final String SEPARATOR_SYMBOL = ",";

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {

		final String[] values = ivalue.toString().split(SEPARATOR_SYMBOL);

		DoubleWritable latitud = null;

		try {
			latitud = new DoubleWritable(Double.parseDouble(values[2]));
		} catch (Exception e) {
			System.out.println("NO ERA UN Número... '" + values[2] + "'");
		}

		if (latitud != null)
			context.write(latitud, NullWritable.get());
	}

}
