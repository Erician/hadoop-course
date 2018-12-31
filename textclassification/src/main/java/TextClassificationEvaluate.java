import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import utils.TextClassificationPreparation;

import java.io.IOException;

public class TextClassificationEvaluate {
    static public void evaluate() throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream in = fs.open(new Path(TextClassificationPreparation.HadoopExpDir+"/"
                +TextClassificationPreparation.EvaluateDir + "/"
                +TextClassificationPreparation.ResultDir + "/"
                +TextClassificationPreparation.PredictFileResultDir + "/"
                +TextClassificationPreparation.PredictFileResult+TextClassificationPreparation.ReduceoutSuffix));
        int TP=0, TN=0,FP=0,FN=0;
        String line = in.readLine();
        String class_name = line.split("\t")[0];
        while(line != null) {
            String []parts = line.split("\t");
            if(parts[0].equals(class_name)) {
                if(parts[2].equals(class_name)){
                    TP++;
                }else {
                    FP++;
                }
            }else {
                if(parts[2].equals(class_name)){
                    FN++;
                }else {
                    TN++;
                }
            }
            line = in.readLine();
        }
        in.close();

        double P, R, F1;
        P = TP*1.0/(TP + FP);
        R = TP*1.0/(TP + FN);
        F1 = 2*P*R/(P + R);
        FSDataOutputStream out = fs.create(new Path(TextClassificationPreparation.HadoopExpDir+"/"
                +TextClassificationPreparation.EvaluateDir + "/"
                +TextClassificationPreparation.EvaluateFileResult));
        out.writeChars(""+P+" "+R+" "+F1);
        out.close();
        fs.close();
    }
}
