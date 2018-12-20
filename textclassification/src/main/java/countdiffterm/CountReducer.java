package countdiffterm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {

    @Override
    public void reduce(TextPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int cnt = 0;
        for(IntWritable value : values) {
            cnt++;
        }
        context.write(key, new IntWritable(cnt));
    }
}
