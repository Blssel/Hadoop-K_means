import org.apache.hadoop.fs.Path;

public class CommonArgument {
	public static final int K=5;//聚类的类别数为5
	public static final String inputlocation="hdfs://localhost:8020/user/NBAData/data.txt";
	public static final String outputlocation="hdfs://localhost:8020/user/NBAData/results";
	public static final String center_inputlocation="hdfs://localhost:8020/user/NBAData/centers.txt";
	public static final String center_outputlocation="hdfs://localhost:8020/user/NBAData/output_centers";
	public static final String new_center_outputlocation="hdfs://localhost:8020/user/NBAData/output_centers/part-r-00000";
	public static final int REPEAT=100;
	public static final float threshold=(float)0.1;
	public static Path inputpath=new Path(inputlocation);
	public static Path outputpath=new Path(outputlocation);
	public static Path center_inputpath=new Path(center_inputlocation);
	public static Path center_outputpath=new Path(center_outputlocation);
}
