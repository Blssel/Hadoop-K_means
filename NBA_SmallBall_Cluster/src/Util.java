import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class Util {
	//中心点的个数
	public static final int K=5;
	//读出中心点，注意必须包括键值
	public static List<ArrayList<String>> getCenterFile(String inputPath){
		List<ArrayList<String>> centers=new ArrayList<ArrayList<String>>();
		Configuration conf=new Configuration();
		try{
			FileSystem fs=CommonArgument.center_inputpath.getFileSystem(conf);
			Path path=new Path(inputPath);
			FSDataInputStream fsIn=fs.open(path);
			//一行一行读取参数，存在Text中，再转化为String类型
			Text lineText=new Text();
			String tmpStr=null;
			LineReader linereader=new LineReader(fsIn,conf);
			while(linereader.readLine(lineText)>0){
				ArrayList<String> oneCenter=new ArrayList<>();
				tmpStr=lineText.toString();
				//分裂String，存于容器中
				tmpStr=tmpStr.replace("\t", " ");
				String[] tmp=tmpStr.split(" ");
				for(int i=0;i<tmp.length;i++){
					oneCenter.add(tmp[i]);
				}
				//将此点加入集合
				centers.add(oneCenter);
			}
			fsIn.close(); 
		}catch(IOException e){
			e.printStackTrace();
		}
		//返回容器
		return centers;
	}
	
	//判断是否满足停止条件
	public static boolean isOK(String inputpath,String outputpath,int k,float threshold)
	throws IOException{
		//获得输入输出文件
		List<ArrayList<String>> oldcenters=Util.getCenterFile(inputpath);
		List<ArrayList<String>> newcenters=Util.getCenterFile(outputpath);
		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		System.out.println(oldcenters.get(0).size());
		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		//累加中心点间的距离
		float distance=0;
		for(int i=0;i<K;i++){
			for(int j=1;j<oldcenters.get(i).size();j++){
				float tmp=Math.abs(Float.parseFloat(oldcenters.get(i).get(j))
						-Float.parseFloat(newcenters.get(i).get(j)));
				distance+=Math.pow(tmp,2);
			}
		}
		/*如果超出阈值，则返回false
		 * 否则更新中心点文件
		*/
		if(distance>threshold)
			return true;
		
		//更新中心点文件
		Util.deleteLastResult(inputpath);//先删除旧文件
		Configuration conf=new Configuration();
		Path ppp=new Path("hdfs://localhost:8020/user/NBAData/");
		FileSystem fs=ppp.getFileSystem(conf);
		//通过local作为中介
		fs.moveToLocalFile(new Path(outputpath), new Path(
						"/home/blssel/Downloads/数据/16-17常规赛nba球队数据/tmp.data"));
		fs.delete(new Path(inputpath), true);//在写入inputpath之前再次确保此文件不存在
		fs.moveFromLocalFile(new Path("/home/blssel/Downloads/数据/16-17常规赛nba球队数据/tmp.data")
						,new Path(inputpath));
		return false;
	}
	
	//删除上一次mapreduce的结果
	public static void deleteLastResult(String inputpath){
		Configuration conf=new Configuration();
		try{
			Path ppp=new Path("inputpath");
			FileSystem fs2= ppp.getFileSystem(conf);
			fs2.delete(new Path(inputpath),true);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
