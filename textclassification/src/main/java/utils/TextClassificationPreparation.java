package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class TextClassificationPreparation {
    public static String HadoopExpDir = "/user/eric/hadoop-exp";
    public static String ClassificationDir = "classification";
    public static String DataDir = "data";
    public static String ResultDir = "result";
    public static String ReduceoutSuffix = "-r-00000";

    public static String CountDiffClassFileResultDir = "count-file-dir";
    public static String CountDiffClassFileResult = "count-file";
    public static String CountDiffTermResultDir = "count-term-dir";
    public static String CountDiffTermResult = "count-term";

    public static String EvaluateDir = "evaluate";
    public static String PredictFileResultDir = "predict-file-dir";
    public static String PredictFileResult = "predict-file";
    public static String EvaluateFileResult = "evaluate-file";

    public static void randomSelectTextsForClassification(String []paths)
        throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(HadoopExpDir + "/" + ClassificationDir)) == true) {
            fs.delete(new Path(HadoopExpDir + "/" + ClassificationDir), true);
        }
        if (fs.exists(new Path(HadoopExpDir + "/" + EvaluateDir)) == true) {
            fs.delete(new Path(HadoopExpDir + "/" +EvaluateDir), true);
        }
        for(String path : paths) {
            for(Path p : FileUtil.stat2Paths(fs.listStatus(new Path(path)))) {
                if (Math.random() > 0.5) {
                    FileUtil.copy(fs, p, fs,
                            new Path(HadoopExpDir + "/" + ClassificationDir + "/" + DataDir + "/"
                                    + getLastlevelDirFromPath(p.toString()) + "/" + p.getName()), false, new Configuration());
                }else {
                    FileUtil.copy(fs, p, fs,
                            new Path(HadoopExpDir + "/" + EvaluateDir + "/" + DataDir + "/"
                                    + getLastlevelDirFromPath(p.toString()) + "/" + p.getName()), false, new Configuration());
                }
            }
        }
        fs.close();
        return;
    }

    public static String getLastlevelDirFromPath(String path) {
        String []parts =  path.split("/");
        return parts[parts.length-2];
    }

    public static String getNameFromPath(String path) {
        String []parts =  path.split("/");
        return parts[parts.length-1];
    }
}
