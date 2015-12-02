import org.apache.commons.collections.KeyValue;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Created by Fuga on 15/12/1.
 */
public class matrix_cal_Mapred {

    public static class matrix_cal_Mapper extends Mapper<Text, Text, Text, IntWritable> {

        @Override
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            SortedMapWritable matrix_a = SortedMapWritableDisplay.StringToSortedMapWritable(key);
            SortedMapWritable matrix_b = SortedMapWritableDisplay.StringToSortedMapWritable(value);

            SortedMapWritable smw = new SortedMapWritable();


            Set<WritableComparable> A_keys = matrix_a.keySet();
            Set<WritableComparable> B_keys = matrix_b.keySet();
            for (Writable A_key : A_keys) {
                Text A_value = (Text) matrix_a.get(A_key);
                for (Writable B_key : B_keys) {
                    Text B_value = (Text) matrix_b.get(B_key);
                    IntWritable iw = new IntWritable(Integer.parseInt(A_value.toString())*Integer.parseInt(B_value.toString()));
                    context.write(new Text(A_key+","+B_key),iw);
                }
            }
        }
    }

    public static class matrix_cal_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int result = 0;
            for (IntWritable val : values) {
                result += val.get();
            }
            context.write(key, new IntWritable(result));
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = new JobConf(MainRun.class);

        conf.setJobName("MatrixMultiplySecondJob");
        conf.addResource("/Users/Fuga/hadoop/hadoop-1.2.1/conf/core-site.xml");
        conf.addResource("/Users/Fuga/hadoop/hadoop-1.2.1/conf/hdfs-site.xml");
        conf.addResource("/Users/Fuga/hadoop/hadoop-1.2.1/conf/mapred-site.xml");
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");

        Path output_path = new Path(path.get("HDFS")+"/user/matrixmultiply/output");
        FileSystem fs = output_path.getFileSystem(conf);
        if (fs.delete(output_path, true)) {
            System.out.println("删除上次output目录成功");
        }
        else {
            System.out.println("删除上次output目录失败");
            System.exit(1);
        }
        Path input = new Path(path.get("HDFS") + "/user/matrixmultiply/matrix/output/part-r-00000");

        Job job = new Job(conf);
        job.setJarByClass(matrix_cal_Mapred.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(matrix_cal_Mapper.class);
        job.setReducerClass(matrix_cal_Reducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output_path);
//        FileInputFormat.setMinInputSplitSize(job, 0);
//        FileInputFormat.setMaxInputSplitSize(job, 1024*1L);

        job.waitForCompletion(true);
    }

}
