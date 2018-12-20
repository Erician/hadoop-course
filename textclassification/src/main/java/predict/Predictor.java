package predict;

import countdiffterm.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.TextClassificationPreparation;

import java.io.*;
import java.util.Iterator;

import java.util.Hashtable;

public class Predictor {

    private static Hashtable<String, Hashtable<String, Double> > conditionProbilityHashtable
             = new Hashtable<String, Hashtable<String, Double> >();
    private static Hashtable<String, Double> priorProbilityHashtable = new Hashtable<String, Double>();
    private static Hashtable<String, Long> termNumInClassHashtable = new Hashtable<String, Long>();
    //private static Hashtable<String, Long> diffTermNumInClassHashtable = new Hashtable<String, Long>();
    private static long allDiffTermNum = 0;
    private static long allTermNum = 0;

    public static void calculateProbility() throws IOException{
        FileSystem fs = FileSystem.get(new Configuration());
        calculatePriorProbility(fs);
        calculateConditionProbility(fs);
        fs.close();
    }

    public static void calculatePriorProbility(FileSystem fs) throws IOException {

        FSDataOutputStream out = fs.create(new Path(TextClassificationPreparation.HadoopExpDir+"/"
                +TextClassificationPreparation.EvaluateDir + "/"
                +TextClassificationPreparation.ResultDir + "/"
                +"priorProbilityHashtable"));

        FSDataInputStream in =  fs.open(new Path(TextClassificationPreparation.HadoopExpDir+"/"
                        +TextClassificationPreparation.ClassificationDir + "/"
                        +TextClassificationPreparation.ResultDir + "/"
                        +TextClassificationPreparation.CountDiffClassFileResultDir + "/"
                        +TextClassificationPreparation.CountDiffClassFileResult
                        +TextClassificationPreparation.ReduceoutSuffix));
        String line = in.readLine();
        long allFiles = 0;
        while(line != null) {
            String []parts = line.split("\t");
            priorProbilityHashtable.put(parts[0], new Double(parts[1]));
            allFiles += Long.valueOf(parts[1]);
            line = in.readLine();
        }
        for(Iterator<String> classIterator=priorProbilityHashtable.keySet().iterator();
            classIterator.hasNext();) {
            String className = classIterator.next();
            priorProbilityHashtable.put(className, priorProbilityHashtable.get(className)*1.0/allFiles);
            out.writeBytes(className + "\t" + priorProbilityHashtable.get(className) + "\n");
        }
        in.close();
        out.close();
        return;
    }

    public static void calculateConditionProbility(FileSystem fs) throws IOException {

        FSDataOutputStream out = fs.create(new Path(TextClassificationPreparation.HadoopExpDir+"/"
                +TextClassificationPreparation.EvaluateDir + "/"
                +TextClassificationPreparation.ResultDir + "/"
                +"conditionProbilityHashtable"));

        FSDataInputStream in =  fs.open(new Path(TextClassificationPreparation.HadoopExpDir+"/"
                +TextClassificationPreparation.ClassificationDir + "/"
                +TextClassificationPreparation.ResultDir + "/"
                +TextClassificationPreparation.CountDiffTermResultDir + "/"
                +TextClassificationPreparation.CountDiffTermResult
                +TextClassificationPreparation.ReduceoutSuffix));

        String line = in.readLine();
        while(line != null) {
            String []parts = line.split("\t");
            if(!conditionProbilityHashtable.containsKey(parts[0])) {
                conditionProbilityHashtable.put(parts[0], new Hashtable<String, Double>());
            }
            Hashtable<String, Double> termAndNum = conditionProbilityHashtable.get(parts[0]);
            termAndNum.put(parts[1], new Double(parts[2]));
            line = in.readLine();
        }
        in.close();

        for(Iterator<String> classIterator=conditionProbilityHashtable.keySet().iterator();
            classIterator.hasNext();) {
            String className = classIterator.next();
            allDiffTermNum += conditionProbilityHashtable.get(className).size();
            long tmpTermCount = 0;
            for(Iterator<String> termIterator=conditionProbilityHashtable.get(className).keySet().iterator();
                termIterator.hasNext();) {
                String term = termIterator.next();
                tmpTermCount += conditionProbilityHashtable.get(className).get(term);
            }
            if(!termNumInClassHashtable.containsKey(className)) {
                termNumInClassHashtable.put(className, tmpTermCount);
            }else{
                termNumInClassHashtable.put(className, termNumInClassHashtable.get(className) + tmpTermCount);
            }
            allTermNum += tmpTermCount;
        }

        for(Iterator<String> classIterator=termNumInClassHashtable.keySet().iterator();
            classIterator.hasNext();) {
            String className = classIterator.next();
            out.writeBytes(className + "\t" + termNumInClassHashtable.get(className) + "\n");
        }

        for(Iterator<String> classIterator=conditionProbilityHashtable.keySet().iterator();
            classIterator.hasNext();){
            String className=classIterator.next();
            for(Iterator<String> termIterator=conditionProbilityHashtable.get(className).keySet().iterator();
                termIterator.hasNext();) {
                String term = termIterator.next();
                Double cnt = conditionProbilityHashtable.get(className).get(term);
                conditionProbilityHashtable.get(className).put(term,
                        (conditionProbilityHashtable.get(className).get(term)+1)*1.0/
                                (termNumInClassHashtable.get(className)));
                out.writeBytes(className + "\t" + term + "\t" + cnt + "\t" +
                        conditionProbilityHashtable.get(className).get(term) + "\n");
            }
        }
        out.writeBytes(allDiffTermNum + "\n");
        out.close();
        return;
    }

    public static class PredictionMapper extends Mapper<Text, Text, Text, predict.TextPair> {

        @Override
        public void map(Text key, Text content, Context context)
                throws IOException, InterruptedException {
            calculateProbility();
            Hashtable<String, Double> probilitiesHashtable = new Hashtable<String, Double>();
            //prior probility
            for(Iterator<String> classIterator=priorProbilityHashtable.keySet().iterator();
                classIterator.hasNext();) {
                String className = classIterator.next();
                probilitiesHashtable.put(className, Math.log(priorProbilityHashtable.get(className)));
            }
            //condition probility
            String []terms = content.toString().split("\n");
            int matchCount = 0;
            for(String term : terms) {
                for(Iterator<String> classIterator=conditionProbilityHashtable.keySet().iterator();
                    classIterator.hasNext();){
                    String className=classIterator.next();
                    double conProbility = 0;
                    if(conditionProbilityHashtable.get(className).containsKey(term)) {
                        conProbility = conditionProbilityHashtable.get(className).get(term);
                        matchCount += 1;
                    }else {
                        conProbility = (1+0)*1.0/(allTermNum);
                    }
                    probilitiesHashtable.put(className, probilitiesHashtable.get(className) + Math.log(conProbility));
                }
            }
            //write
            for(Iterator<String> classIterator=probilitiesHashtable.keySet().iterator();
                classIterator.hasNext();) {
                String className = classIterator.next();
                //context.write(key, key);
                context.write(new Text(key.toString()+"\t"+Integer.toString(terms.length)), new predict.TextPair(new Text(className), new DoubleWritable(probilitiesHashtable.get(className))));
            }
        }
    }

    public static class PredictionReducer extends Reducer<Text, predict.TextPair, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<predict.TextPair> values, Context context)
                throws IOException, InterruptedException {
            double biggestPro = 0;
            String classNameCorrespingingToBiggestPro = "";
            for(predict.TextPair value : values) {
                if(value.getProbility().get() > biggestPro){
                    biggestPro = value.getProbility().get();
                    classNameCorrespingingToBiggestPro = value.getClassName().toString();
                }
                context.write(key, new Text(value.getClassName().toString() + "\t"
                        + value.getProbility().toString()));
            }
            //context.write(key, new Text(classNameCorrespingingToBiggestPro + "\t"
            //+ Double.toString(biggestPro)));
        }
    }
}
