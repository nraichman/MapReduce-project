import java.io.IOException; 
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer; 


public class InvertReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
		Set<String> indices = new HashSet<>();
		for (Text t : values) {
			indices.add(t.toString());
		}
		String word = key.toString();
		List<String> res = new ArrayList<String>(indices);
		Collections.sort(res);
		ctx.write(new Text(String.format("%s -> %s", word, String.join(",", res))), null);
	}
}