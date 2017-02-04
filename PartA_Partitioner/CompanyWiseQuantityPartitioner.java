package acadgild.Partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CompanyWiseQuantityPartitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numPartition) {
		int firstChar = Character.toUpperCase(key.toString().charAt(0));
		if(firstChar >= 'A' && firstChar <= 'F')
			return 0;
		if(firstChar >= 'G' && firstChar <= 'L')
			return 1;
		if(firstChar >= 'M' && firstChar <= 'R')
			return 2;
		
		return 3;
	}

}
