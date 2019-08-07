import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable IN_WRITABLE_ONE = new IntWritable(1);
    private Text word = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line);

        while (stringTokenizer.hasMoreTokens()) {

            word.set(stringTokenizer.nextToken());
            context.write(word, IN_WRITABLE_ONE);
        }
    }
}
