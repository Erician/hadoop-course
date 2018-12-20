package countdiffclassfile;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    //static String CountFilesResultNamePrefix = "count-files-";
    //private MultipleOutputs<Text, IntWritable> multipleOutputs;
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int cnt = 0;
        for(IntWritable value : values) {
            cnt++;
        }
        context.write(key, new IntWritable(cnt));
    }

//    @Override
//    protected void setup(Context context)throws IOException, InterruptedException {
//        multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
//    }
//
//    @Override
//    protected void cleanup(Context context)throws IOException, InterruptedException {
//        multipleOutputs.close();
//    }

}
