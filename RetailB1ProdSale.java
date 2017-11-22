

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




public class ProdSale {
	//retail
	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(";");	 
	            long sales=Long.parseLong(str[8]);
	            
	            
	            
	            context.write(new Text(str[5]),new LongWritable(sales));
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	         
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,LongWritable,LongWritable,Text>
	   {
		  	public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
		      	long sum=0;
		 
		      	 for (LongWritable val : values)
		         {       	
		        	 sum+=val.get();
		         }
		       	         		     
		     context.write(new LongWritable(sum),key);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	  public static class SortMapper extends Mapper<LongWritable,Text,LongWritable,Text>
		{
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
			{
				String[] valueArr = value.toString().split("\t");
				context.write(new LongWritable(Long.parseLong(valueArr[0])), new Text(valueArr[1]));
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
		    Job job = Job.getInstance(conf, "Highest Amount");
		    job.setJarByClass(ProdSale.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(1);
		    job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);
		    job.setOutputKeyClass(LongWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    //FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    Path outputPath1 = new Path("FirstMapper");
			FileOutputFormat.setOutputPath(job, outputPath1);
			FileSystem.get(conf).delete(outputPath1, true);
			job.waitForCompletion(true);
		    
		    Job job3 = Job.getInstance(conf,"Per Occupation - totalTxn");
			job3.setJarByClass(ProdSale.class);
			job3.setMapperClass(SortMapper.class);
			job3.setReducerClass(SortReducer.class);
			job3.setSortComparatorClass(DecreasingComparator.class);
			job3.setMapOutputKeyClass(LongWritable.class);
			job3.setMapOutputValueClass(Text.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(LongWritable.class);
			FileInputFormat.addInputPath(job3, outputPath1);
			FileOutputFormat.setOutputPath(job3, new Path(args[1]));
			FileSystem.get(conf).delete(new Path(args[1]), true);
			System.exit(job3.waitForCompletion(true) ? 0 : 1);
		  }
}


