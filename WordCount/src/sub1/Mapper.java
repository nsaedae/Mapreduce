package sub1;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


// Map ������ �����ϴ� Ŭ����
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer st = new StringTokenizer(value.toString());
		
		while(st.hasMoreTokens()) {
			
			String word = st.nextToken();
			
			Text txt = new Text(word);
			IntWritable val = new IntWritable(1);
			
			// split �ܾ key, value ������ ���
			context.write(txt, val);
		}
		
		
	}
}








