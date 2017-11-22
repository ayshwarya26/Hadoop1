




import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

//import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;





public class RetailC2Category {
	//retail
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(";");	 
	            
	            long sales=Long.parseLong(str[8]);
	            long cost=Long.parseLong(str[7]);
	            long loss=cost-sales;
	            String aaa=str[2]+","+loss;
	            if(loss>0)
	            {
	            context.write(new Text(str[4]),new Text(aaa));
	            }
	            }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	         
	      }
	   }
	 public static class CaderPartitioner extends
     Partitioner < Text, Text >
	   {
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	         String[] str = value.toString().split(",");
	         String age=str[0];


	         if(age.contains("A"))
	         {
	            return 0 % numReduceTasks;
	         }
	         else if(age.contains("B"))
	         {
	            return 1 % numReduceTasks ;
	         }
	         else if(age.contains("C"))
	         {
	            return 2 % numReduceTasks;
	         }
	         
	         else if(age.contains("D"))
	         {
	            return 3 % numReduceTasks;
	         }
	         else if(age.contains("E"))
	         {
	            return 4 % numReduceTasks;
	         }
	         else if(age.contains("F"))
	         {
	            return 5 % numReduceTasks;
	         }
	         else if(age.contains("G"))
	         {
	            return 6 % numReduceTasks;
	         }
	         else if(age.contains("H"))
	         {
	            return 7 % numReduceTasks;
	         }
	         else if(age.contains("I"))
	         {
	            return 8 % numReduceTasks;
	         }
	         else if(age.contains("J"))
	         {
	            return 	9 % numReduceTasks;
	         }
	         
	         else 
	       
	         {
	            return 10 % numReduceTasks;
	         }
	        
	         
	      }
	   }
	  public static class ReduceClass extends Reducer<Text,Text,LongWritable,Text>
	   {
		  private LongWritable result = new LongWritable();
		    private Text result1 = new Text();
		    
		  	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      	long sum=0;
		      	 String st= "";
		      	 for (Text val : values)
		         {     	
		      		String [] str = val.toString().split(",");
		      		LongWritable value = new LongWritable(Integer.parseInt(str[1]));
		        	 
		        	 sum += value.get(); 
		        	 st=key+"\t"+str[0];
		         }
		      	result.set(sum);
			      result1.set(st);
			      
			      context.write(result,result1);	         		     
		     //context.write(new LongWritable(sum),key);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	  public static class SortMapper extends Mapper<LongWritable,Text,LongWritable,Text>
		{
		  private Text result3 = new Text();
		
		    
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
			{
				String[] v = value.toString().split("\t");
				String st=v[1]+","+v[2];
				
				result3.set(st);
				
				context.write(new LongWritable(Long.parseLong(v[0])),result3);
			}
		}

	  public static class CaderPartitioner2 extends
	     Partitioner < LongWritable	, Text >
		   {
		      public int getPartition(LongWritable key, Text value, int numReduceTasks)
		      {
		         String[] str = value.toString().split(",");
		         String age=str[1];


		         if(age.contains("A"))
		         {
		            return 0 % numReduceTasks;
		         }
		         else if(age.contains("B"))
		         {
		            return 1 % numReduceTasks ;
		         }
		         else if(age.contains("C"))
		         {
		            return 2 % numReduceTasks;
		         }
		         
		         else if(age.contains("D"))
		         {
		            return 3 % numReduceTasks;
		         }
		         else if(age.contains("E"))
		         {
		            return 4 % numReduceTasks;
		         }
		         else if(age.contains("F"))
		         {
		            return 5 % numReduceTasks;
		         }
		         else if(age.contains("G"))
		         {
		            return 6 % numReduceTasks;
		         }
		         else if(age.contains("H"))
		         {
		            return 7 % numReduceTasks;
		         }
		         else if(age.contains("I"))
		         {
		            return 8 % numReduceTasks;
		         }
		         else if(age.contains("J"))
		         {
		            return 	9 % numReduceTasks;
		         }
		         
		         else 
		       
		         {
		            return 10 % numReduceTasks;
		         }
		        
		         
		      }
		   }
	  public static class SortReducer extends Reducer<LongWritable,Text,Text,LongWritable>
		{
			public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException
			{
				for(Text val : value)
				{
					context.write(new Text(val), key);
				}
			}
		}

	 
	  public static void main(String[] args) throws Exception {
		  Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job1 = Job.getInstance(conf, "Volume Count");
		    job1.setJarByClass(RetailC2Category.class);
		    job1.setMapperClass(MapClass.class);
		    job1.setPartitionerClass(CaderPartitioner.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job1.setReducerClass(ReduceClass.class);
		    job1.setNumReduceTasks(11);
		    job1.setMapOutputKeyClass(Text.class);
		    job1.setMapOutputValueClass(Text.class);
		    job1.setOutputKeyClass(LongWritable.class);
		    job1.setOutputValueClass(Text.class);
		    Path outputPath2 = new Path("FirstMapper");
		    FileInputFormat.addInputPath(job1, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job1, outputPath2);
		    FileSystem.get(conf).delete(outputPath2, true);
		    //FileOutputFormat.setOutputPath(job1, new Path(args[1]));
			//FileSystem.get(conf).delete(new Path(args[1]), true);
			
			job1.waitForCompletion(true);
			
			Job job3 = Job.getInstance(conf,"Per Occupation - totalTxn");
			job3.setJarByClass(RetailC2Category.class);
			job3.setMapperClass(SortMapper.class);
			job3.setPartitionerClass(CaderPartitioner2.class);
			job3.setNumReduceTasks(11);
			job3.setReducerClass(SortReducer.class);
			job3.setSortComparatorClass(DecreasingComparator.class);
			job3.setMapOutputKeyClass(LongWritable.class);
			job3.setMapOutputValueClass(Text.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(LongWritable.class);
			FileInputFormat.addInputPath(job3, outputPath2);
			FileOutputFormat.setOutputPath(job3, new Path(args[1]));
			FileSystem.get(conf).delete(new Path(args[1]), true);
			System.exit(job3.waitForCompletion(true) ? 0 : 1);

			
			
			
					  }
}

