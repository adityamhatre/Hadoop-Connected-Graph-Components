package edu.uta.cse6331;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

/*
 * Created by Aditya on October 08, 2017.
 */

public class Graph {
    public static void main(String[] args) {
        try {
            ToolRunner.run(new Job1(), args);
            for (int i = 0; i < 5; i++) {
                ToolRunner.run(new Job2(i), args);
            }
            ToolRunner.run(new Job3(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class Vertex implements Writable {
    short tag;                  // 0 for a graph vertex, 1 for a group number
    long group;                 // the group where this vertex belongs to
    long VID;                   // the vertex ID
    Vector<Long> adjacent;      // the vertex neighbors

    Vertex(short tag, long group, long VID, Vector<Long> adjacent) {
        this.tag = tag;
        this.group = group;
        this.VID = VID;
        this.adjacent = adjacent;
    }

    Vertex(short tag, long group) {
        this.tag = tag;
        this.group = group;
        this.adjacent = new Vector<>();
    }

    Vertex() {
        this.adjacent = new Vector<>();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeShort(tag);
        dataOutput.writeLong(group);
        dataOutput.writeLong(VID);
        System.out.println("WRITING SIZE = " + (adjacent == null ? 0 : adjacent.size()));
        dataOutput.writeInt(adjacent == null ? 0 : adjacent.size());
        for (Long anAdjacent : adjacent) {
            System.out.println("WRITING ELEMENT = " + (anAdjacent));
            dataOutput.writeLong(anAdjacent);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tag = dataInput.readShort();
        group = dataInput.readLong();
        VID = dataInput.readLong();
        int size = dataInput.readInt();
        //if (adjacent == null) adjacent = new Vector<>(size);
        System.out.println("READING SIZE = " + size);
        for (int i = 0; i < size; i++) {
            long elem = dataInput.readLong();
            System.out.println("READING ELEMENT = " + (elem));
            adjacent.add(elem);
        }
    }

    Vertex copy() {
        Vector<Long> copy = new Vector<>(this.adjacent.size());
        copy.addAll(this.adjacent);
        return new Vertex(this.tag, this.group, this.VID, copy);
    }
}


class Mapper1 extends Mapper<Object, Text, LongWritable, Vertex> {
    protected void map(Object key, Text line, Context context) throws IOException, InterruptedException {
        String[] split = line.toString().split(",");
        long VID = Long.parseLong(split[0]);
        Vector<Long> adjacent = new Vector<>();
        for (int i = 1; i < split.length; i++) {
            adjacent.add(Long.parseLong(split[i]));
        }
        System.out.println("LOGECT:------\nMapper 1: <(" + VID + "), (0, " + VID + ", " + VID + ", " + adjacent.toString() + ")>");
        context.write(new LongWritable(VID), new Vertex((short) 0, VID, VID, adjacent));
    }
}

class Mapper2 extends Mapper<Object, Vertex, LongWritable, Vertex> {
    protected void map(Object key, Vertex vertex, Context context) throws IOException, InterruptedException {
        System.out.println("LOGECT:------\nMapper 2: <(" + vertex.VID + "), (" + vertex.tag + ", " + vertex.group + ", " + vertex.VID + ", " + vertex.adjacent.toString() + ")>");
        context.write(new LongWritable(vertex.VID), vertex.copy());
        for (long n : vertex.adjacent) {
            //System.out.println("LOGECT:------\nMapper 2 in loop: <(" + n + "), (1, " + vertex.group + ")>");
            context.write(new LongWritable(n), new Vertex((short) 1, vertex.group));
        }
    }
}

class Reducer2 extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {

    protected void reduce(LongWritable vid, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
        long m = Long.MAX_VALUE;
        Vertex foundVertex = null;
        for (Vertex v : values) {
            if (v.tag == 0)
                foundVertex = v.copy();
            m = Math.min(m, v.group);
        }
        System.out.println("LOGECT:------\nReducer 2: <(" + m + "), (0, " + m + ", " + vid.get() + ", " + foundVertex.adjacent.toString() + ")>");
        context.write(new LongWritable(m), new Vertex((short) 0, m, vid.get(), foundVertex.adjacent));
    }
}

class Mapper3 extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {
    protected void map(LongWritable group, Vertex value, Context context) throws IOException, InterruptedException {
        System.out.println("LOGECT:------\nMapper 3: <(" + group.get() + "), (1)>");
        context.write(new LongWritable(group.get()), new LongWritable(1));
    }
}

class Reducer3 extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
    protected void reduce(LongWritable group, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long m = 0;
        for (LongWritable v : values)
            m += v.get();
        System.out.println("LOGECT:------\nReducer 3: <(" + group.get() + "), (" + m + ")>");
        context.write(new LongWritable(group.get()), new LongWritable(m));
    }
}

class Job1 extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job1 = Job.getInstance(getConf());

        job1.setJarByClass(Vertex.class);

        job1.setMapperClass(Mapper1.class);
        job1.setNumReduceTasks(0);

        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);

        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/f0"));

        return job1.waitForCompletion(true) ? 0 : 1;
    }
}

class Job2 extends Configured implements Tool {
    private int i;

    Job2(int i) {
        this.i = i;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job2 = Job.getInstance(getConf());

        job2.setJarByClass(Vertex.class);

        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);

        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Vertex.class);

        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Vertex.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[1] + "/f" + i));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/f" + (i + 1)));

        return job2.waitForCompletion(true) ? 0 : 1;
    }
}

class Job3 extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job3 = Job.getInstance(getConf());

        job3.setJarByClass(Vertex.class);

        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);

        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);

        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);

        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job3, new Path(args[1] + "/f5"));
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));

        return job3.waitForCompletion(true) ? 0 : 1;
    }
}

