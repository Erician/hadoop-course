package predict;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {

    private Text className;
    private DoubleWritable probility;

    public TextPair() {
        this.className = new Text();
        this.probility = new DoubleWritable();
    }

    public TextPair(String className, double probility) {
        this.className = new Text(className);
        this.probility = new DoubleWritable(probility);
    }

    public TextPair(Text className, DoubleWritable probility) {
        this.className = className;
        this.probility = probility;
    }

    public TextPair(TextPair tp) {
        this.className = tp.className;
        this.probility = tp.probility;
    }

    public void set(Text className, DoubleWritable probility) {
        this.className = className;
        this.probility = probility;
    }

    public Text getClassName() {
        return this.className;
    }

    public DoubleWritable getProbility() {
        return this.probility;
    }

    public void write(DataOutput out) throws IOException {
        this.className.write(out);
        this.probility.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.className.readFields(in);
        this.probility.readFields(in);
    }

    @Override
    public int hashCode() {
        return this.className.hashCode()*163 + this.probility.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TextPair) {
            TextPair tp = (TextPair)o;
            return this.className.equals(tp.className) && this.probility.equals(tp.probility);
        }
        return false;
    }

    @Override
    public String toString() {
        return this.className + "\t" + this.probility;
    }


    public int compareTo(TextPair tp) {
        int cmp = this.className.compareTo(tp.className);
        if(cmp != 0) {
            return cmp;
        }
        return this.probility.compareTo(tp.probility);
    }

}
