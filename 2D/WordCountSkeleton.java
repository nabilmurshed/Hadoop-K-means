import java.util.*;
import java.io.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount{

		/*  MAP CLASS */
		
		public static class Map extends MapReduceBase implements Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
		
			//KEYIN and VALUEIN are datatypes that must agree with the InputFormat's datatypes
			//KEYOUT and VALUEOUT are datatypes that may be any type (dictated by the program's logic)
			
			public void map(KEYIN key, VALUEIN value, OutputCollector<KEYOUT, VALUEOUT> output, Reporter report) throws IOException {
			
				//apply program's logic on key and value
				//emit intermediate key and value to reducers
			}
		}
		
		/* REDUCE CLASS */
		
		public static class Reduce extends MapReduceBase implements Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
		
			//KEYIN and VALUEIN are datatypes that must agree with KEYOUT and VALUEOUT of the MAP class
			//KEYOUT and VALUEOUT are datatypes that may be of any type (dictated by the program's logic)
			
			public void reduce(KEYIN key, Iterator<VALUEIN> values, OutputCollector<KEYOUT, VALUEOUT> output, Reporter report) throws IOException
			
				//apply programs logic on key and list of values
				//emit final key and value to output file
			}
		}
		
		public static void main(String args[]) throws Exception{
		
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("WordCount Example");
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setOutputKeyClass(KEYOUT.class);
		conf.setOutputValueClass(VALUEOUT.class);
		
		conf.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
		
}
