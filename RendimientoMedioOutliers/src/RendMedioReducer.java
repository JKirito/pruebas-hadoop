import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import entities.CeldaWritable;
import funciones.OperadorRend;

public class RendMedioReducer extends Reducer<CeldaWritable, DoubleWritable, CeldaWritable, DoubleWritable> {

	public void reduce(CeldaWritable _key, Iterable<DoubleWritable> values, Context context) throws IOException,
			InterruptedException {

		OperadorRend oper = new OperadorRend(values);

		context.write(_key, new DoubleWritable(oper.sumaSinOutliers(0.1, 0.95)));
	}
}
