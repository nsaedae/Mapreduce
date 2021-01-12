package sub1;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


// Map 연산을 수행하는 클래스
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		
		String line = value.toString();
		String[] tokens = line.split(",");
		
		try {
			String region = tokens[0];
			double temp = Double.parseDouble(tokens[5]); 
			
			context.write(new Text(region), new DoubleWritable(temp));
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}








