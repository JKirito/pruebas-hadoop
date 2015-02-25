import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Latitud_MinMaxReducer extends Reducer<DoubleWritable, NullWritable, DoubleWritable, NullWritable> {

	public void reduce(DoubleWritable _key, Iterable<NullWritable> values, Context context) throws IOException,
			InterruptedException {
		// process values
		// for (Text val : values) {
		//
		// }
		context.write(_key, NullWritable.get());
	}
}
