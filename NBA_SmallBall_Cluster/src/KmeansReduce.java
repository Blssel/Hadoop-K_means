
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;

public class KmeansReduce extends Reducer<Text, Text, Text,Text> {
	public void reduce(Text key,Iterable<Text> value,Context context)
			throws IOException,InterruptedException{
		//把value值放进String数组中（只获取和处理一个）
		long num=0;
		double threePointAtmp=0;
		double pace=0;
		for(Text T:value){
			num++;
			String onePoint=T.toString();
			onePoint=onePoint.replace("\t", " ");
			String[] parameters=onePoint.split(" ");
			//进行累加
			//for(int i=0;i<parameters.length;i++){
				threePointAtmp+=Double.parseDouble(parameters[1]);
				pace+=Double.parseDouble(parameters[2]);
			//}
		}
		System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		System.out.println(threePointAtmp);
		System.out.println(pace);
		System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		double avg_threePointAtmp=threePointAtmp/((double)num);
		double avg_pace=pace/((double)num);
		String result=avg_threePointAtmp+" "+avg_pace;
		context.write(key,new Text(result));
	}
}
