// Anvita Yellamanchali
// Mapper for data profiling

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.StringBuilder;

public class CleanMapper 
    extends Mapper<LongWritable, Text, Text, Text> {

    int id = 0;
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
        // Text value is one line of data
        String line = value.toString();
        String col[] = line.split(",");

        // Don't add header, duplicate U.S. rows in dataset
        if (!col[0].equals("Exposure Id") && !col[5].equals("United States of America")) {
            // Only write columns with all fields complete
            if (col.length == 14) {
                StringBuilder neededColumns = new StringBuilder();

                // Add year
                neededColumns.append(col[9]);
                neededColumns.append(",");
                // Add country
                if (col[2].equals("United States of America")) {
                    // Match name to join with another dataset
                    neededColumns.append("United States");
                } else {
                    neededColumns.append(col[2]);
                }
                neededColumns.append(",");
                // Add socio-demographic index
                neededColumns.append(col[5]);
                neededColumns.append(",");
                // Add pollutant
                neededColumns.append(col[10]);
                neededColumns.append(",");
                // Add exposure lower
                neededColumns.append(col[6]);
                neededColumns.append(",");
                // Add exposure upper
                neededColumns.append(col[8]);
                neededColumns.append(",");
                // Add exposure mean
                neededColumns.append(col[7]);
    
                context.write(new Text(""), new Text(neededColumns.toString()));
            }
        }
    }
}