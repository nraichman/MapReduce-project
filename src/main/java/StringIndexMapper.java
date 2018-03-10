import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper;

public class StringIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		@SuppressWarnings("resource")
		Scanner token = new Scanner(value.toString());
		if (token.hasNext()) {
			Text index = new Text(token.next().split(",")[0]);
			Text word;
			while (token.hasNext()) {
				String s = StringUtils.cleanWord(token.next());
				if (s.length() > 2) {
					word = new Text(s);
					Counter.addIndex(index.toString());
					context.write(word, index);
				}
			}
		}
	}
}