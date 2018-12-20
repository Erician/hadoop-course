package countdiffterm;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMapper extends Mapper<NullWritable, TextPair, TextPair, IntWritable> {

    @Override
    public void map(NullWritable key, TextPair classTerm, Context context)
            throws IOException, InterruptedException {
        context.write(new TextPair(classTerm), new IntWritable(1));
    }
}