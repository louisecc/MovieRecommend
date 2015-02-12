package recommend;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

public class Recommend {

    public static final String HDFS = "hdfs://localhost:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main(String[] args) throws Exception {
        Map<String, String> path = new HashMap<String, String>();
        path.put("data", "logfile/small.csv");
        path.put("1stInput", HDFS + "/user/hdfs/recommend");
        path.put("1stOutput", path.get("1stInput") + "/1st");
        path.put("2ndInput", path.get("1stOutput"));
        path.put("2ndOutput", path.get("1stInput") + "/2nd");
        path.put("3rdInput1", path.get("1stOutput"));
        path.put("3rdOutput1", path.get("1stInput") + "/3rd_1");
        path.put("3rdInput2", path.get("2ndOutput"));
        path.put("3rdOutput2", path.get("1stInput") + "/3rd_2");       
        path.put("4thInput1", path.get("3rdOutput1"));
        path.put("4thInput2", path.get("3rdOutput2"));
        path.put("4thOutput", path.get("1stInput") + "/4th");       
        path.put("5thInput1", path.get("3rdOutput1"));
        path.put("5thInput2", path.get("3rdOutput2"));
        path.put("5thOutput", path.get("1stInput") + "/5th");
        path.put("6thInput", path.get("5thOutput"));
        path.put("6thOutput", path.get("1stInput") + "/6th");

        UserMovieAggregation.run(path);
        MovieMovieAggregation.run(path);
        MatrixMultiply.run1(path);
        MatrixMultiply.run2(path);
        FinalResult_1.run(path);
        FinalResult_2.run(path);
        System.exit(0);
    }
}
