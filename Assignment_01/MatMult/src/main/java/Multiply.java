
import java.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.ArrayList;
import java.util.Scanner;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;

    Pair() {
    }

    Pair(int i, int j) {
        this.i = i;
        this.j = j;
    }

    /* ... */
    public void write(DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }

    /* ... */
    public void readFields(DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }

    /* ... */
    @Override
    public int compareTo(Pair inPairs) {
        if (i != inPairs.i) {
            return Integer.compare(i, inPairs.i);
        } else {
            return Integer.compare(j, inPairs.j);
        }
    }

    @Override
    public String toString() {
        return i + "\t" + j;
    }

}

public class Multiply extends Configured implements Tool {

    /* ... */
    public static class MatElem implements Writable {
        short tag;
        int index;
        double value;

        public MatElem() {
        }

        public MatElem(short tag, int index, double value) {
            this.tag = tag;
            this.index = index;
            this.value = value;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeShort(tag);
            out.writeInt(index);
            out.writeDouble(value);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            tag = in.readShort();
            index = in.readInt();
            value = in.readDouble();
        }

        @Override
        public String toString() {
            return tag + "\t" + index + "\t" + value;
        }
    }

    // First Map task
    public static class FirstMapperTaskM extends Mapper<Object, Text, IntWritable, MatElem> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            Scanner input = new Scanner(value.toString()).useDelimiter("\\s+");
            int i = input.nextInt();
            int j = input.nextInt();
            double v = input.nextDouble();
            context.write(new IntWritable(j), new MatElem((short) 0, i, v));
            input.close();
        }
    }

    // Second Map task
    public static class FirstMapperTaskN extends Mapper<Object, Text, IntWritable, MatElem> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            Scanner input = new Scanner(value.toString()).useDelimiter("\\s+");
            int i = input.nextInt();
            int j = input.nextInt();
            double v = input.nextDouble();
            context.write(new IntWritable(i), new MatElem((short) 1, j, v));
            input.close();
        }
    }

    // First Reducer task
    public static class FirstReducerTask extends Reducer<IntWritable, MatElem, Pair, DoubleWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<MatElem> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<MatElem> m = new ArrayList<>();
            ArrayList<MatElem> n = new ArrayList<>();
            Configuration config = context.getConfiguration();
            for (MatElem e : values) {
                MatElem tmp = ReflectionUtils.newInstance(MatElem.class, config);
                ReflectionUtils.copy(config, e, tmp);
                if (e.tag == 0) {
                    m.add(tmp);
                } else if (e.tag == 1) {
                    n.add(tmp);
                }
            }
            for (MatElem e1 : m) {
                for (MatElem e2 : n) {
                    Pair p = new Pair(e1.index, e2.index);
                    double multiplyResult = e1.value * e2.value;
                    context.write(p, new DoubleWritable(multiplyResult));
                }
            }
        }
    }

    // Second Map task
    public static class SecondMapperTask extends Mapper<Pair, DoubleWritable, Pair, DoubleWritable> {
        @Override
        public void map(Pair key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    // Second Reducer task
    public static class SecondReducerTask extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
        @Override
        public void reduce(Pair key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    // Delete Record Function
    public static boolean deleteRecord(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteRecord(file);
                    } else {
                        file.delete();
                    }
                }
            }
        }
        return dir.delete();
    }

    @Override
    public int run(String[] args) throws Exception {
        /* ... */
        return 0;
    }

    public static void main(String[] args) throws Exception {
        File tmp = new File("Intermediate");
        deleteRecord(tmp);
        Configuration config = new Configuration();
        Job firstJob = Job.getInstance(config, "firstJob");
        firstJob.setJarByClass(Multiply.class);
        firstJob.setMapOutputKeyClass(IntWritable.class);
        firstJob.setMapOutputValueClass(MatElem.class);
        MultipleInputs.addInputPath(firstJob, new Path(args[0]), TextInputFormat.class,
                FirstMapperTaskM.class);
        MultipleInputs.addInputPath(firstJob, new Path(args[1]), TextInputFormat.class,
                FirstMapperTaskN.class);
        firstJob.setReducerClass(FirstReducerTask.class);
        firstJob.setOutputKeyClass(Pair.class);
        firstJob.setOutputValueClass(DoubleWritable.class);
        firstJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(firstJob, new Path(args[2]));
        firstJob.waitForCompletion(true);
        Job secondJob = Job.getInstance(config, "secondJob");
        secondJob.setJarByClass(Multiply.class);
        secondJob.setMapperClass(SecondMapperTask.class);
        secondJob.setMapOutputKeyClass(Pair.class);
        secondJob.setMapOutputValueClass(DoubleWritable.class);
        secondJob.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(secondJob, new Path(args[2]));
        secondJob.setReducerClass(SecondReducerTask.class);
        secondJob.setOutputKeyClass(Pair.class);
        secondJob.setOutputValueClass(DoubleWritable.class);
        secondJob.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(secondJob, new Path(args[3]));
        secondJob.waitForCompletion(true);
    }
}
