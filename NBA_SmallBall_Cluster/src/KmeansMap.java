import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansMap extends Mapper<LongWritable, Text, Text, Text> {
	public static final int num = 2;// 定义对象维度
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 首先将value中的数字提取出来，放到容器当中
		String data = value.toString();
		data=data.replace("\t", " ");
		String[] tmpSplit = data.split(" ");
		System.out.println("#########################################################");
		System.out.println(tmpSplit.length);
		System.out.println("#########################################################");
		List<String> parameter = new ArrayList<>();
		for (int i = 0; i < tmpSplit.length; i++) {
			parameter.add(tmpSplit[i]);
		}
		// 读取中心点文件(其中路径参数是从命令中获取的)
		List<ArrayList<String>> centers = Util.getCenterFile(CommonArgument.center_inputlocation);
		// 计算目标对象到各个中心点的距离，找最大距离对应的中心点，则认为此对象归到该点中
		String outKey="" ;// 默认聚类中心为0号中心点
		double minDist = Double.MAX_VALUE;
		System.out.println("**********************************************************");
		System.out.println(centers.size());
		System.out.println("**********************************************************");
		for (int i = 0; i < centers.size(); i++) {
			// 由于是二维数据，因此要累加两次
			double dist = 0;
			for(int j=1;j<centers.get(i).size();j++){
				double a=Double.parseDouble(parameter.get(j));
				double b=Double.parseDouble(centers.get(i).get(j));
				dist+=Math.pow(a-b,2);
			}
			if (dist < minDist) {
				//outKey = Integer.parseInt(centers.get(i).get(0));// 中心点文件中所写的标号
				outKey = centers.get(i).get(0);// 中心点文件中所写的标号
				minDist = dist;
			}
			System.out.println("");
			System.out.println(dist);
			System.out.println("");
		}
		System.out.println("----------------------------------------");
		System.out.println(outKey+"+");
		System.out.println("----------------------------------------");
		context.write(new Text(outKey), value);
	}
}
