import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        if (line != null && line.length() != 0) {
//            context.write(new Text(line), new IntWritable(1));
            for (String word : line.split(new String(" "))) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }
}
