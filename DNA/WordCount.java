import java.util.*;
import java.io.*;
import java.net.*;

import org.apache.hadoop.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount
{
	//To get the current iteration of K means algorithm
	public static int getIterCount() throws Exception
	{
		try{
            Path pt=new Path("hdfs://mnabilmu-n01.qatar.cmu.local:9000/user/hadoop/metadata/iter.txt");
            FileSystem fs2 = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs2.open(pt)));
            String line;
            line=br.readLine();
            if(line != null)
            	return Integer.parseInt(line);
            br.close();       
        }catch(Exception e){}
        return 0;
	}

	/*
		Helper function for averageDNA. Returns the index of the largest array element.
		@params array if integers.
		@return int that represents the index.
	*/
	public static int frequent(int[] ATGC)
	{
		int maxfreq = 0;
		int maxfreqIdx = 0;
		for(int i = 0;i<4;i++)
		{
			if(ATGC[i]>maxfreq)
			{
				maxfreq = ATGC[i];
				maxfreqIdx = i;
			}
		}
		return maxfreqIdx;
	}
	/*
		Generates an avergae DNA by taking the highest protein at each index.
		For every DNA strand at index i, it checks for the most popular protein.
		@params list of DNA Strings whose average DNA has to be found.
		@return String that represents the average DNA.
	*/
	public static String averageDNA(ArrayList<String> list)
	{
		String result = "";
		int[] ATGC = {0,0,0,0};
		int size = list.size();
		int DNAlen = list.get(0).length();
		for(int k = 0;k<DNAlen;k++)
		{
			for(String s : list)
			{
				if(s.charAt(k)=='A')
					ATGC[0]++;
				else if(s.charAt(k)=='T')
					ATGC[1]++;
				else if(s.charAt(k)=='G')
					ATGC[2]++;
				else 
					ATGC[3]++;
			}
			int maxfreqIdx = frequent(ATGC);
			if(maxfreqIdx==0)
				result=result+"A";
			else if(maxfreqIdx==1)
				result=result+"T";
			else if(maxfreqIdx==2)
				result=result+"G";
			else 
				result=result+"C";
		}
		return result;
	}

	/*
		Computes the number of non same characters in two strands.
		@params two strings that will be compared.
		@return int representing the num of differences.
	*/
	public static int editDist(String x,String y)
	{
		int dist = 0;
		for(int i = 0;i<x.length();i++)
		{
			if(x.charAt(i)!=y.charAt(i))
				dist++;
		}
		return dist;
	}

	/*  MAP CLASS */		
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		//KEYIN and VALUEIN are datatypes that must agree with the InputFormat's datatypes
		//KEYOUT and VALUEOUT are datatypes that may be any type (dictated by the program's logic)

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter report) throws IOException 
		{
			String line = value.toString();
			Scanner scanner = new Scanner(line);

			//processing each point from input file
			while(scanner.hasNext())
			{
				String current = scanner.next();

				String line2;
				int minDist = Integer.MAX_VALUE;
				int minCentIndex = 0;
				try{
                    Path pt=new Path("hdfs://mnabilmu-n01.qatar.cmu.local:9000/user/hadoop/metadata/centroid.txt");
                    FileSystem fs3 = FileSystem.get(new Configuration());
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs3.open(pt)));
                    line2=br.readLine();
                    int currentIter = getIterCount();

                    //Finds the nearest centroid
                    while (line2 != null){
                    	String[] tempcent = line2.split(",");
                    	int numIter = Integer.parseInt(tempcent[0]);
                    	int centIndex = Integer.parseInt(tempcent[1]);
                    	String centDNA = tempcent[2];

                    	if(numIter == currentIter)
                    	{
                    		int tempDist = editDist(current,centDNA);
                    		if(tempDist<minDist)
                    		{
                    			minDist = tempDist;
                    			minCentIndex = centIndex;
                    		}
                    	}

                        line2=br.readLine();
                    }//end of centroid iterator
                    br.close();
                }catch(Exception e){}

				output.collect(new Text(minCentIndex+""), new Text(current));
			}//end of points iterator
		}
	}
		
	/* REDUCE CLASS */
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {
	
		//KEYIN and VALUEIN are datatypes that must agree with KEYOUT and VALUEOUT of the MAP class
		//KEYOUT and VALUEOUT are datatypes that may be of any type (dictated by the program's logic)

		//add exception if needed
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter report) throws IOException{
		
			int count = 0;             //Counts the number of points in the cluster
			ArrayList<String> list = new ArrayList<String>();

			int numIter = 0;
			try{
				numIter = getIterCount()+1;
			}
			catch(Exception e){System.out.println("iter.txt could not be read");}

			String centbuff = "";
			try{
                Path centfilepath=new Path("hdfs://mnabilmu-n01.qatar.cmu.local:9000/user/hadoop/metadata/centroid.txt");
                FileSystem centreader = FileSystem.get(new Configuration());
                BufferedReader cbr=new BufferedReader(new InputStreamReader(centreader.open(centfilepath)));
                String aline;
                aline=cbr.readLine();
                if (aline != null){
                 	centbuff = aline;
                 	aline=cbr.readLine();
                 } 
                while (aline != null){
                	centbuff = centbuff+"\n"+aline;
                    aline=cbr.readLine();
                }
                cbr.close();
            }catch(Exception e){}


			try{
				Path pt=new Path("hdfs://mnabilmu-n01.qatar.cmu.local:9000/user/hadoop/metadata/centroid.txt");
	            FileSystem fs = FileSystem.get(new Configuration());
	            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true))); //reset the old file

				//per DNA in the cluster
				while(values.hasNext())
				{
					String templine = (values.next()).toString();
					list.add(templine);
					count ++;
				}
				//Reassign centroid DNA coordinates to Average coordinates
				//Writing updated centroids to the centroid.txt file. Format: CentroidID,Centroid DNA
				 String line = centbuff+"\n"+numIter+","+key.toString()+","+averageDNA(list);
                br.write(line);
                br.close();
            }catch(Exception e){
                System.out.println("centroid.txt file not found");
        	}
			
			//emit final key and value to output file
			output.collect(key, new IntWritable(count));
		}
	}
		
	public static void main(String args[]) throws Exception
	{
		long start = System.currentTimeMillis();

		int numCluster = Integer.parseInt(args[2]);

		try{
			Path pointfilepath=new Path("hdfs://mnabilmu-n01.qatar.cmu.local:9000/user/hadoop/dna/input/input.txt");
            FileSystem readcent = FileSystem.get(new Configuration());
            BufferedReader ptbuff=new BufferedReader(new InputStreamReader(readcent.open(pointfilepath)));
            String ptline;
            ptline=ptbuff.readLine();

            Path centfilepath=new Path("hdfs://mnabilmu-n01.qatar.cmu.local:9000/user/hadoop/metadata/centroid.txt");
            FileSystem writecent = FileSystem.get(new Configuration());
            BufferedWriter centbuff=new BufferedWriter(new OutputStreamWriter(writecent.create(centfilepath,true)));

            while (ptline != null && numCluster>0){

                //Writing the centroid to the file
            	String centline = "0,"+numCluster+","+ptline+"\n";
            	centbuff.write(centline);

            	numCluster--;

                ptline=ptbuff.readLine();
            }
            ptbuff.close();
            centbuff.close();
        }catch(Exception e){System.out.println("Writing centroids unsuccessful, Please change the name of input file to input.txt ");}

		int numIter = 0;
		//Increments the current number of iteration completed
	
		for (int i = 0;i<2 ;i++ ) {
			try{
	            Path pt3=new Path("hdfs://mnabilmu-n01.qatar.cmu.local:9000/user/hadoop/metadata/iter.txt");
	            FileSystem fs1 = FileSystem.get(new Configuration());
	            BufferedWriter br3=new BufferedWriter(new OutputStreamWriter(fs1.create(pt3,true))); 
	            String linex = ""+numIter;
	            br3.write(linex);
	            br3.close();
        	}catch(Exception e){
            	System.out.println("Could not write iter.txt");
        	}

		
			JobConf conf = new JobConf(WordCount.class);
			conf.setJobName("WordCount Example");
			
			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);
			
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			
			conf.setNumReduceTasks(1);
			
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]+i));
			
			JobClient.runJob(conf);
			numIter++;
		}

		long end = System.currentTimeMillis();
		System.out.println((end-start)*0.001);
	}
}
