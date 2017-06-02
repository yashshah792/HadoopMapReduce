import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ProblemTwo
{
	//MapperOne - divides into station name|season|year-temp(MMNT)
	public static class MapperOne extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	  {
			String arr = value.toString();
            String[] strlist = arr.split(",");
            String kainai = null;
            String Key="", Value="";
            String state = strlist[5];
            String gender = strlist[69];
            String salary = strlist[72];
//            System.out.println("State: "+state+" || gender: "+gender+" || salary: "+salary);
//            if(salary.equals("bbbbbb") || salary.equals("N/A") || salary.equals("N/A") || salary.equals(kainai)){
            if(salary.isEmpty() || salary.equals("WAGP")){	
            	salary = "NaN";
//            	System.out.println("Inside condition");
            }
            if(gender.equals("1")){
            	gender ="Male";
//            	System.out.println("Gender in male"+gender);
            }
            else if(gender.equals("2")){
            	gender = "Female";
//            	System.out.println("Gender in female"+gender);
            }
            
            Key = state+"_"+gender;
//            System.out.println("Key:  "+ Key);
            Value = salary;
//            System.out.println("Value:  "+ Value);
            context.write(new Text(Key), new Text(Value));
	  }
	}
	
	//ReducerOne - Computes avg of temp per year per season
	public static class ReducerOne extends Reducer<Text, Text, Text, Text>
	{
		@Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{	
			String Values = "";
			double temptotal = 0;
			System.err.println("temptotal:"+temptotal);
			String kainai = null;
			int count = 0;
			for(Text val : values){
				String str = val.toString();
				System.out.println("str::"+str);
				if(!str.equals("NaN") && !str.equals("WAGP")){
					double temp = Double.parseDouble(str);
					temptotal += temp;
					System.out.println("temp total"+temptotal+"     temp::"+temp);
					count++;
				}
			}
			
			double average = temptotal/count;
			Values += Double.toString(average);
			
			context.write(key, new Text(Values));
	}
}
	
	public static void main(String[] args) throws Exception
	{
		long st = System.currentTimeMillis();
		JobConf conf = new JobConf(ProblemTwo.class);
		Job j1 = new Job(conf);
		
		Path in1 = new Path(args[0]);
		Path in2 = new Path(args[1]);
		Path out1in2 = new Path(args[2]+"ProblemTwo");
		conf.setNumMapTasks(2);
		conf.setNumReduceTasks(4);
		j1.setJobName("Stage 1: Section wise station vectors");
		j1.setJarByClass(ProblemTwo.class);

		//Input and Output type for Mapper
		j1.setMapOutputKeyClass(Text.class);
		j1.setMapOutputValueClass(Text.class);

		//Output type for Reducer
		j1.setOutputKeyClass(Text.class);
		j1.setOutputValueClass(Text.class);

		//The Input and Output type for the whole program
		j1.setInputFormatClass(TextInputFormat.class);
		j1.setOutputFormatClass(TextOutputFormat.class);
		
		//Setting up mapper one
		j1.setMapperClass(MapperOne.class);

		//Setting up Reducer one
		j1.setReducerClass(ReducerOne.class);
		
		//Input and Output file paths for first set of mapper and reducers
		MultipleInputs.addInputPath(j1,in1,TextInputFormat.class,MapperOne.class);
		MultipleInputs.addInputPath(j1,in2,TextInputFormat.class,MapperOne.class);
		FileOutputFormat.setOutputPath(j1, out1in2);
		
		//Submitting the job
		int flag = j1.waitForCompletion(true) ? 0 : 1;

		System.out.println("Number of mappers: "+conf.get("mapred.map.tasks"));
		System.out.println("Number of reducers: "+conf.get("mapred.reduce.tasks"));
		long et = System.currentTimeMillis();
		System.out.println("Computation time " + (et-st));
		System.exit(flag);
		
 	}
}
