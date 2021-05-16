// Anvita Yellamanchali
// Reducer for data profiling

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountRecsReducer 
    extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    @Override
    // Calls reduce for each <key, (list of values)> pair in the grouped inputs
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException {

        // Find maximum and minimum of mean exposure (of the different pollutant types)
        if (key.toString().startsWith("exposure mean")) {
            double minValue = Double.MAX_VALUE;
            double maxValue = Double.MIN_VALUE;

            for (DoubleWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
                minValue = Math.min(minValue, value.get());
            }
            context.write(new Text("Maximum " + key), new DoubleWritable(maxValue));
            context.write(new Text("Minimum " + key), new DoubleWritable(minValue));

        } else { // Count the number of records
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            // Write the column name and the total count of records
            context.write(key, result);
        }
	}
}