package spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import java.util.List;

public class SparkSearch {
	private static ScriptEngineManager manager = new ScriptEngineManager();
    private static ScriptEngine engine = manager.getEngineByName("js");
    private static String translated = "";
    private static SparkSession spark;
    private static String fileName;
    
    public SparkSearch(String fileName) {
    	spark = SparkSession.builder().master("local").appName("Spark Search").getOrCreate();
    	this.fileName = fileName;
    }
    
    public static List<String> makeQuery(String query) throws ScriptException {
    	Dataset<String> set = spark.read().textFile(fileName).cache();
    	set = evalLoop(set, query);
    	List<String> results = set.as(Encoders.STRING()).collectAsList();
    	return results;
    }
    
    private static Dataset<String> evalLoop(Dataset<String> set, String query) throws ScriptException {
		String[] terms = query.split(" and ");
		for (String term: terms) {
			if (term.contains("(")) {
	    		set = set.filter(s -> evaluate(s, term.substring(1, term.length()- 1)));
	    	}
	    	else {
	    		set = set.filter(s -> evaluate(s, term));
	    	}
		}
		return set;
	}
    
    private static boolean evaluate(String s, String term) throws ScriptException {
		engine.put("s", s);
		String[] parts = term.split(" or ");
		String predicate = "";
		for(int i=0; i<parts.length - 1; i++) {
			String part = "term" + i;
			engine.put(part, parts[i]);
			if (parts[i].substring(0, 3).equals("not(")) {
				predicate += "!s.contains(term" + i + ") || ";
			}
			else {
				predicate += "s.contains(term" + i + ") || ";
			}
		}
		//The case where length = n
		String part = "term" + (parts.length-1);
		engine.put(part, parts[parts.length-1]);
		if (parts[parts.length-1].substring(0, 3).equals("not(")) {
			predicate += "!s.contains(term" + (parts.length-1) + ")";
		}
		else {
			predicate += "s.contains(term" + (parts.length-1) + ")";
		}
//		return predicate;
		Object result = engine.eval(predicate);
		return Boolean.TRUE.equals(result);
	}
}
