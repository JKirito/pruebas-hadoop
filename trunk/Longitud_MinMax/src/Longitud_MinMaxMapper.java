import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Longitud_MinMaxMapper extends Mapper<LongWritable, Text, DoubleWritable, NullWritable> {

	private static final String SEPARATOR_SYMBOL = ",";

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {

		final String[] values = ivalue.toString().split(SEPARATOR_SYMBOL);

		DoubleWritable longitud = null;

		try {
			longitud = new DoubleWritable(Double.parseDouble(values[1]));
		} catch (Exception e) {
			System.out.println("NO ERA UN Número... '" + values[1] + "'");
		}

		if (longitud != null)
			context.write(longitud, NullWritable.get());
	}

}
