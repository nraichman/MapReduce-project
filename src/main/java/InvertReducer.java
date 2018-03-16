import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer;



public class InvertReducer extends Reducer<Text, Text, Text, Text> {
	@SuppressWarnings("unchecked")
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
		Map<String, Integer> rankMap = new HashMap<>();
		for (Text t : values) {
			int curVal = rankMap.get(t.toString()) == null ? 1 : rankMap.get(t.toString()) + 1;
			rankMap.put(t.toString(), curVal);
		}
		List<Map.Entry<String, Integer>> rankList = new LinkedList<>(rankMap.entrySet());
		@SuppressWarnings("unchecked")
		Object[] rankedArray = sortEntryList(rankList.toArray(), 0, rankList.size());
		
		String word = key.toString();
		List<String> res = new LinkedList<String>();
		for(int i = 0; i < rankedArray.length; i++) {
			res.add(((Map.Entry<String, Integer>) rankedArray[i]).getKey());
		}
		ctx.write(new Text(String.format("%s -> %s", word, String.join(",", res))), null);
	}
	
	private synchronized Object[] sortEntryList(Object[] entryList, int start, int end) {
		int middle = (start + end)/2;
		if(start == end) {
			return entryList;
		}else {
			sortEntryList(entryList, start, middle);
			sortEntryList(entryList, middle+1, end);
			return merge(entryList, start, middle+1, end);
		}
	}
	
	@SuppressWarnings("unchecked")
	private synchronized Object[] merge(Object[] entryList, int start, int middle, int end){
		int i = start;
		int j = middle;
		int m = 0;
		Object[] mergedArray = new Object[entryList.length];
		while(i < middle && j < end) {
			if(((Map.Entry<String, Integer>) entryList[i]).getValue() < ((Map.Entry<String, Integer>) entryList[j]).getValue()) {
				mergedArray[m] = entryList[i];
				m++;
				i++;
			}else {
				mergedArray[m] = entryList[j];
				m++;
				j++;
			}
		}
		while(i < middle) {
			mergedArray[m] = entryList[i];
			m++;
			i++;
		}
		while(j < end) {
			mergedArray[m] = entryList[j];
			m++;
			j++;
		}
		return mergedArray;
	}
}