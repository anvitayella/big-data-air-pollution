// Anvita Yellamanchali
// Mapper for data profiling

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountRecsMapper 
    extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final static DoubleWritable one = new DoubleWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
        // Text value is one line of data
        String line = value.toString();
        String data[] = line.split(",");

        // Count total number of lines
        context.write(new Text("Total Lines"), one);

        // Only count lines with all fields complete
        if (data.length == 14) {
            // Ignore title line for country
            if (!data[2].equals("Country")) {
                context.write(new Text("Countries"), one);
            }

            // Ignore title line for socio-demographic index
            if (!data[5].equals("Name")) {
                context.write(new Text("SDI"), one);
            }

            // Ignore title line for exposure lower
            if (!data[6].equals("Exposure Lower")) {
                context.write(new Text("Lower Exposures"), one);
            }

            // Ignore title line for exposure mean
            if (!data[7].equals("Exposure Mean")) {
                context.write(new Text("Mean Exposures"), one);

                // To find maximum and minimum mean exposure
                double exposureMean = Double.parseDouble(data[7]);
                context.write(new Text("exposure mean " + data[10]), new DoubleWritable(exposureMean));
            }

            // Ignore title line for exposure upper
            if (!data[8].equals("Exposure Upper")) {
                context.write(new Text("Upper Exposures"), one);
            }

            // Ignore title line for year
            if (!data[9].equals("Year")) {
                context.write(new Text("Years"), one);
            }

            // Ignore title line for pollutant
            if (!data[10].equals("Pollutant")) {
                context.write(new Text("Pollutants"), one);
            }
        }
    }
}
