import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		File folder = new File(args[0]);
		Path out = new Path(args[1]);
		Path inp;

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "inverted index");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);

		for (File fileEntry : folder.listFiles()) {
			if (fileEntry.isFile()) {
				inp = new Path(fileEntry.getPath());
		        TextInputFormat.addInputPath(job, inp);
			}
	    }

		job.setJarByClass(Driver.class);
		job.setMapperClass(StringIndexMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(InvertReducer.class);

		job.waitForCompletion(true);
	}
}