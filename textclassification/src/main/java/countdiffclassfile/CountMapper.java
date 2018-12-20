package countdiffclassfile;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMapper extends Mapper<NullWritable, Text, Text, IntWritable> {

    @Override
    public void map(NullWritable key, Text fileClass, Context context)
            throws IOException, InterruptedException {
        context.write(new Text(fileClass), new IntWritable(1));
    }
}