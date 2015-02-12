package recommend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import myhadoop.HdfsDAO;

public class FinalResult_2 {

    public static class FinalResultRecommendMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            Text k = new Text(tokens[0]);
            Text v = new Text(tokens[1]+","+tokens[2]);
            context.write(k, v);
        }
    }

    public static class FinalResultRecommendReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> map = new HashMap<String, Double>();
            
            for (Text line : values) {
                String[] tokens = Recommend.DELIMITER.split(line.toString());
                String movieID = tokens[0];
                Double score = Double.parseDouble(tokens[1]);
                
                 if (map.containsKey(movieID)) {
                     map.put(movieID, map.get(movieID) + score);
                 } else {
                     map.put(movieID, score);
                 }
            }
            
            Iterator<String> iter = map.keySet().iterator();
            while (iter.hasNext()) {
                String movieID = iter.next();
                double score = map.get(movieID);
                Text v = new Text(movieID + "," + score);
                context.write(key, v);
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Recommend.config();

        String input = path.get("6thInput");
        String output = path.get("6thOutput");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf);
        job.setJarByClass(FinalResult_2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(FinalResult_2.FinalResultRecommendMapper.class);
        job.setReducerClass(FinalResult_2.FinalResultRecommendReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

}
