import java.io.IOException; 
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer; 


public class InvertReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
		HashMap<String, Integer> indices = new HashMap<>();
		for (Text t : values) {
			String v = t.toString();
			indices.put(v, indices.getOrDefault(v, 0) + 1);
		}
		String word = key.toString();
		ArrayList<String> res = new ArrayList<String>(indices.keySet());
		if (res.size() < Counter.getCount()) {
			res.sort((String a, String b) -> indices.get(b) - indices.get(a));
			Collections.sort(res);
			ctx.write(new Text(String.format("%s -> %s", word, String.join(",", res))), null);
		}
	}
}