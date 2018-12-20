package predict;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.TextClassificationPreparation;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

public class PredictionMapper extends Mapper<Text, BytesWritable, Text, TextPair> {

    private Hashtable<String, Hashtable<String, Double> > conditionProbilityHashtable
            = new Hashtable<String, Hashtable<String, Double> >();
    private Hashtable<String, Double> priorProbilityHashtable = new Hashtable<String, Double>();
    private Hashtable<String, Long> termNumInClassHashtable = new Hashtable<String, Long>();
    private long allDiffTermNum = 0;
    private long allTermNum = 0;

    public void calculateProbility() throws IOException{
        FileSystem fs = FileSystem.get(new Configuration());
        calculatePriorProbility(fs);
        calculateConditionProbility(fs);
        fs.close();
    }

    public void calculatePriorProbility(FileSystem fs) throws IOException {

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
        }
        in.close();
        return;
    }

    public void calculateConditionProbility(FileSystem fs) throws IOException {

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
            }
        }
        return;
    }

    @Override
    public void map(Text key, BytesWritable content, Context context)
            throws IOException, InterruptedException {

        calculateProbility();
        Hashtable<String, Double> probilitiesHashtable = new Hashtable<String, Double>();
        //prior probility
        for(Iterator<String> classIterator = priorProbilityHashtable.keySet().iterator();
            classIterator.hasNext();) {
            String className = classIterator.next();
            probilitiesHashtable.put(className, Math.log(priorProbilityHashtable.get(className)));
        }
        //condition probility
        String []terms = new String(content.getBytes()).split("\r\n");
        for(String term : terms) {
            //term = term.substring(0, term.length()-1);
            for(Iterator<String> classIterator=conditionProbilityHashtable.keySet().iterator();
                classIterator.hasNext();){
                String className=classIterator.next();
                double conProbility = 0;
                if(conditionProbilityHashtable.get(className).containsKey(term)) {
                    conProbility = conditionProbilityHashtable.get(className).get(term);
                    System.out.println(className+"\t"+term+"\t"+"1"+"\t"+Math.log(conProbility)+"\n");
                }else {
                    conProbility = (1+0)*1.0/(allTermNum);
                    System.out.println(className+"\t"+term+"\t"+"0"+"\t"+Math.log(conProbility)+"\n");
                }
                probilitiesHashtable.put(className, probilitiesHashtable.get(className) + Math.log(conProbility));
                System.out.println(className+"\t"+probilitiesHashtable.get(className)+"\n");
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
