// Anvita Yellamanchali
// Reducer for data profiling

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanReducer 
    extends Reducer<Text, Text, Text, Text> {

    @Override
    // Calls reduce for each <key, (list of values)> pair in the grouped inputs
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
        
        for (Text value : values) {
            context.write(new Text(""), value);
        }
	}
}
