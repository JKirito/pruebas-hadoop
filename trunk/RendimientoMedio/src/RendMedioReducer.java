import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import entities.CeldaWritable;

public class RendMedioReducer extends Reducer<CeldaWritable, DoubleWritable, CeldaWritable, DoubleWritable> {

	public void reduce(CeldaWritable _key, Iterable<DoubleWritable> values, Context context) throws IOException,
			InterruptedException {
		System.out.println(_key.toString());
		// process values
		double total = 0.0;
		int cantValores = 0;
		for (DoubleWritable val : values) {
			total += val.get();
			cantValores++;
		}
		double media = total / cantValores;

		context.write(_key, new DoubleWritable(media));
	}
}
