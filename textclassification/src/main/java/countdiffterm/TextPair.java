package countdiffterm;

import java.io.*;
import org.apache.hadoop.io.*;

public class TextPair implements WritableComparable<TextPair> {

    private Text className;
    private Text term;

    public TextPair() {
        this.className = new Text();
        this.term = new Text();
    }

    public TextPair(String className, String term) {
        this.className = new Text(className);
        this.term = new Text(term);
    }

    public TextPair(Text className, Text term) {
        this.className = className;
        this.term = term;
    }

    public TextPair(TextPair tp) {
        this.className = tp.className;
        this.term = tp.term;
    }

    public void set(Text className, Text term) {
        this.className = className;
        this.term = term;
    }

    public Text getClassName() {
        return this.className;
    }

    public Text getTerm() {
        return this.term;
    }

    public void write(DataOutput out) throws IOException {
        this.className.write(out);
        this.term.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.className.readFields(in);
        this.term.readFields(in);
    }

    @Override
    public int hashCode() {
        return this.className.hashCode()*163 + this.term.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TextPair) {
            TextPair tp = (TextPair)o;
            return this.className.equals(tp.className) && this.term.equals(tp.term);
        }
        return false;
    }

    @Override
    public String toString() {
        return this.className + "\t" + this.term;
    }


    public int compareTo(TextPair tp) {
        int cmp = this.className.compareTo(tp.className);
        if(cmp != 0) {
            return cmp;
        }
        return this.term.compareTo(tp.term);
    }

}
