package predict;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PredictionReducer extends Reducer<Text, TextPair, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<predict.TextPair> values, Context context)
            throws IOException, InterruptedException {
        double biggestPro = -100000000;
        String classNameCorrespingingToBiggestPro = "";
        for(predict.TextPair value : values) {
            if(value.getProbility().get() > biggestPro){
                biggestPro = value.getProbility().get();
                classNameCorrespingingToBiggestPro = value.getClassName().toString();
            }

        }
        context.write(key, new Text(classNameCorrespingingToBiggestPro));
    }
}
