package countdiffterm;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ClassTermRecordReader
    extends RecordReader <NullWritable, TextPair> {

    private FileSplit fileSplit;
    private FSDataInputStream in;
    private String className;
    private Configuration conf;
    private TextPair value = new TextPair();
    private boolean processed = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
        this.fileSplit = (FileSplit)split;
        this.conf = context.getConfiguration();

        Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(conf);
        in = fs.open(file);
        String [] parts = fileSplit.getPath().toString().split("/");
        className = parts[parts.length-2];
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed) {
            String line = in.readLine();
            if(line == null){
                processed = true;
                return false;
            }
            value.set(new Text(className), new Text(line));
            return true;
        }else {
            return false;
        }
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public TextPair getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        // do nothing
        in.close();
    }
}
