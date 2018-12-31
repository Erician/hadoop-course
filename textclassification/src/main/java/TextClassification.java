import countdiffclassfile.WholeFileInputFormat;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.TextClassificationPreparation;

public class TextClassification {
    public static void main(String []args) throws Exception{
        if (args.length != 2) {
            System.out.println("Usage: textclassification <input path> <input path>");
            System.exit(-1);
        }
        int jobCnt = 0;
        TextClassificationPreparation.randomSelectTextsForClassification(args);
        calculateFileCount(args);
        calculateTermCount(args);
        executePrediction(args);
        TextClassificationEvaluate.evaluate();
    }

    private static void calculateFileCount(String []paths) throws Exception {
        Job job = new Job();
        job.setJarByClass(TextClassification.class);
        job.setJobName("count diff class files");
        
        job.setInputFormatClass(countdiffclassfile.WholeFileInputFormat.class);
        for(String path : paths) {
            WholeFileInputFormat.addInputPath(job,
                    new Path(TextClassificationPreparation.HadoopExpDir+"/"
                            +TextClassificationPreparation.ClassificationDir + "/"
                            +TextClassificationPreparation.DataDir + "/"
                            +TextClassificationPreparation.getNameFromPath(path)));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(TextClassificationPreparation.HadoopExpDir+"/"
                        +TextClassificationPreparation.ClassificationDir + "/"
                        +TextClassificationPreparation.ResultDir + "/"
                        +TextClassificationPreparation.CountDiffClassFileResultDir));

        job.getConfiguration().set("mapreduce.output.basename", TextClassificationPreparation.CountDiffClassFileResult);

        job.setMapperClass(countdiffclassfile.CountMapper.class);
        job.setCombinerClass(countdiffclassfile.CountReducer.class);
        job.setReducerClass(countdiffclassfile.CountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if(!job.waitForCompletion(true)){
            System.exit(-1);
        }
        return ;
    }

    private static void calculateTermCount(String []paths) throws Exception {
        Job job = new Job();
        job.setJarByClass(TextClassification.class);
        job.setJobName("count diff terms");

        job.setInputFormatClass(countdiffterm.WholeFileInputFormat.class);
        for(String path : paths) {
            WholeFileInputFormat.addInputPath(job,
                    new Path(TextClassificationPreparation.HadoopExpDir+"/"
                            +TextClassificationPreparation.ClassificationDir + "/"
                            +TextClassificationPreparation.DataDir + "/"
                            +TextClassificationPreparation.getNameFromPath(path)));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(TextClassificationPreparation.HadoopExpDir+"/"
                        +TextClassificationPreparation.ClassificationDir + "/"
                        +TextClassificationPreparation.ResultDir + "/"
                        +TextClassificationPreparation.CountDiffTermResultDir));

        job.getConfiguration().set("mapreduce.output.basename", TextClassificationPreparation.CountDiffTermResult);

        job.setMapperClass(countdiffterm.CountMapper.class);
        job.setCombinerClass(countdiffterm.CountReducer.class);
        job.setReducerClass(countdiffterm.CountReducer.class);

        job.setOutputKeyClass(countdiffterm.TextPair.class);
        job.setOutputValueClass(IntWritable.class);

        if(!job.waitForCompletion(true)){
            System.exit(-1);
        }
        return ;
    }

    private static void executePrediction(String []paths) throws Exception {
        Job job = new Job();
        job.setJarByClass(TextClassification.class);
        job.setJobName("predict files");

        job.setInputFormatClass(predict.WholeFileInputFormat.class);
        for(String path : paths) {
            WholeFileInputFormat.addInputPath(job,
                    new Path(TextClassificationPreparation.HadoopExpDir+"/"
                            +TextClassificationPreparation.EvaluateDir + "/"
                            +TextClassificationPreparation.DataDir + "/"
                            +TextClassificationPreparation.getNameFromPath(path)));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(TextClassificationPreparation.HadoopExpDir+"/"
                        +TextClassificationPreparation.EvaluateDir + "/"
                        +TextClassificationPreparation.ResultDir + "/"
                        +TextClassificationPreparation.PredictFileResultDir));

        job.getConfiguration().set("mapreduce.output.basename", TextClassificationPreparation.PredictFileResult);

        job.setMapperClass(predict.PredictionMapper.class);
        job.setReducerClass(predict.PredictionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(predict.TextPair.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true)){
            System.exit(-1);
        }
        return ;
    }
}

