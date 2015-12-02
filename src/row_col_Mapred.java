import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Fuga on 15/12/1.
 */
public class row_col_Mapred {

    public static class row_col_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private String flag; // m1 or m2

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit)context.getInputSplit();
            flag = split.getPath().getName();
            System.out.println(flag);

            String[] tokens = MainRun.DELIMITER.split(value.toString());
            if (flag.equals("m1.csv")) {
                Text v = new Text("A:" + tokens[0] + "," + tokens[2]);
                Text k = new Text(tokens[1]);
                context.write(k, v);
                System.out.println(k.toString() + " " + v.toString());
            }
            else if (flag.equals("m2.csv")) {
                Text v = new Text("B:" + tokens[1] + "," + tokens[2]);
                Text k = new Text(tokens[0]);
                context.write(k, v);
                System.out.println(k.toString() + " " + v.toString());
            }
        }
    }

    public static class row_col_Reducer extends Reducer<Text, Text, SortedMapWritableDisplay, SortedMapWritableDisplay> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            SortedMapWritableDisplay mapwritableA = new SortedMapWritableDisplay();
            SortedMapWritableDisplay mapwritableB = new SortedMapWritableDisplay();
            for (Text line: values) {
                String val = line.toString();
                String[] kv = MainRun.DELIMITER.split(val.substring(2));
                if (val.startsWith("A:")) {
                    mapwritableA.put(new IntWritable(Integer.parseInt(kv[0])), new Text(kv[1]));// kv[1] : value, kv[0] : row_n for A and col_n for B
                }
                else if (val.startsWith("B:")) {
                    mapwritableB.put(new IntWritable(Integer.parseInt(kv[0])), new Text(kv[1]));
                }
            }
            context.write(mapwritableA, mapwritableB);
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = MainRun.config();

        String input1 = path.get("input1");
        String input2 = path.get("input2");
        String output = path.get("output");

        Job job = new Job(conf);
        job.setJarByClass(row_col_Mapred.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(SortedMapWritableDisplay.class);
        job.setOutputValueClass(SortedMapWritableDisplay.class);

        job.setMapperClass(row_col_Mapper.class);
        job.setReducerClass(row_col_Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));// 加载2个输入数据集
        FileOutputFormat.setOutputPath(job, new Path(output));
//        FileInputFormat.setMinInputSplitSize(job, 0);
//        FileInputFormat.setMaxInputSplitSize(job, 1024*1L);

        job.waitForCompletion(true);

        matrix_cal_Mapred.run(path);
    }
}
