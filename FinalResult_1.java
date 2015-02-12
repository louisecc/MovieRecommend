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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import myhadoop.HdfsDAO;

public class FinalResult_1 {

    public static class FinalResultMultiplyMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String flag;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();
        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());

            if (flag.equals("3rd_2")) {
                String[] v1 = tokens[0].split(":");
                String movieID1 = v1[0];
                String movieID2 = v1[1];
                String num = tokens[1];

                Text k = new Text(movieID1);
                Text v = new Text("A:" + movieID2 + "," + num);
                
                context.write(k, v);
            } else if (flag.equals("3rd_1")) {
                String[] v2 = tokens[1].split(":");
                String movieID = tokens[0];
                String userID = v2[0];
                String pref = v2[1];

                Text k = new Text(movieID);
                Text v = new Text("B:" + userID + "," + pref);
                
                context.write(k, v);
            }
        }

    }

    public static class FinalResultMultiplyReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, String> mapA = new HashMap<String, String>();
            Map<String, String> mapB = new HashMap<String, String>();

            for (Text line : values) {
                String val = line.toString();
                System.out.println(val);

                if (val.startsWith("A:")) {
                    String[] kv = Recommend.DELIMITER.split(val.substring(2));
                    mapA.put(kv[0], kv[1]);

                } else if (val.startsWith("B:")) {
                    String[] kv = Recommend.DELIMITER.split(val.substring(2));
                    mapB.put(kv[0], kv[1]);

                }
            }

            double result = 0;
            Iterator<String> iter = mapA.keySet().iterator();
            while (iter.hasNext()) {
                String mapk = iter.next();
                // courseID

                int num = Integer.parseInt(mapA.get(mapk));
                Iterator<String> iterb = mapB.keySet().iterator();
                while (iterb.hasNext()) {
                    String mapkb = iterb.next();
                    // stuID
                    double pref = Double.parseDouble(mapB.get(mapkb));
                    result = num * pref;

                    Text k = new Text(mapkb);
                    Text v = new Text(mapk + "," + result);
                    context.write(k, v);
                }
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Recommend.config();

        String input1 = path.get("5thInput1");
        String input2 = path.get("5thInput2");
        String output = path.get("5thOutput");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf);
        job.setJarByClass(FinalResult_1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(FinalResult_1.FinalResultMultiplyMapper.class);
        job.setReducerClass(FinalResult_1.FinalResultMultiplyReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

}
