package sub1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	private List<Double> tempList = new ArrayList<>();
	
	@Override
	protected void reduce(Text region, Iterable<DoubleWritable> values,
			org.apache.hadoop.mapreduce.Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		double temp = 0;
		
		for(DoubleWritable val : values) {
			temp = val.get();
		}
		
		tempList.add(temp);
		
		DoubleWritable result = new DoubleWritable(temp);
		context.write(region, result);
	}
	
	@Override
	protected void cleanup(
			org.apache.hadoop.mapreduce.Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		double total = 0;
		int size = tempList.size();
		
		for(double temp : tempList) {
			total += temp;
		}
		
		context.write(new Text("ÃÑ : "+size+"°Ç"), null);
		context.write(new Text("Æò±Õ¿Âµµ : "+total/size+"µµ"), null);
	}
}



