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

public class ProblemOne
{
	//MapperOne - divides into station name|season|year-temp(MMNT)
	public static class MapperOne extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	  {
			String arr = value.toString();
            String[] strlist = arr.split(",");
            String Key="", Value="";
            String state = strlist[0];
            String strage = strlist[8];
            int age = 0;
            if(!strage.equals("AGEP")){
            	age = Integer.parseInt(strage);
            }
            String group ="";

            if (age >= 0 && age < 10){
				group = "0to9";
			}
			else if(age >= 10 && age < 20){
				group = "10to19";
			}
			else if(age >= 20 && age < 30){
				group = "20to29";
			}
			else if(age >= 30 && age < 40){
				group = "30to39";
			}
			else if(age >= 40 && age < 50){
				group = "40to49";
			}
			else if(age >= 50 && age < 60){
				group = "50to59";
			}
			else if(age >= 60 && age < 70){
				group = "60to69";
			}
			else if(age >= 70 && age < 80){
				group = "70to79";
			}
			else if(age >= 80 && age < 90){
				group = "80to89";
			}
			else if(age >= 90 && age <99){
				group = "90to99";
			}
			else{
				group = "Other";
			}
            Key = state.substring(0, 4)+"_"+group;
            Value = strage;
            context.write(new Text(Key), new Text(Value));
	  }
	}
	
	//ReducerOne - Computes avg of temp per year per season
	public static class ReducerOne extends Reducer<Text, Text, Text, Text>
	{
		@Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{	
			int i1=0,i2=0,i3=0,i4=0,i5=0,i6=0,i7=0,i8=0,i9=0,i10=0;
			String Values = "";
			int i = 0;
			for(Text val : values){
				int age = 0;
				i++;
				if(!val.toString().equals("AGEP")){
					age = Integer.parseInt(val.toString());	
				}
				if (age >= 0 && age < 10){
					i1++;
					Values = Integer.toString(i1);
				}
				else if(age >= 10 && age < 20){
					i2++;
					Values = Integer.toString(i2);
				}
				else if(age >= 20 && age < 30){
					i3++;
					Values = Integer.toString(i3);
				}
				else if(age >= 30 && age < 40){
					i4++;
					Values = Integer.toString(i4);
				}
				else if(age >= 40 && age < 50){
					i5++;
					Values = Integer.toString(i5);
				}
				else if(age >= 50 && age < 60){
					i6++;
					Values = Integer.toString(i6);
				}
				else if(age >= 60 && age < 70){
					i7++;
					Values = Integer.toString(i7);
				}
				else if(age >= 70 && age < 80){
					i8++;
					Values = Integer.toString(i8);
				}
				else if(age >= 80 && age < 90){
					i9++;
					Values = Integer.toString(i9);
				}
				else if(age >= 90 && age < 100){
					i10++;
					Values = Integer.toString(i10);
				}
				else{
					
				}
			}
			System.out.println("Count "+i);
			context.write(key, new Text(Values));
	}
}
	
	public static void main(String[] args) throws Exception
	{
		long st = System.currentTimeMillis();
		JobConf conf = new JobConf(ProblemOne.class);
		Job j1 = new Job(conf);
		
		Path in1 = new Path(args[0]);
		Path in2 = new Path(args[1]);
		Path out1in2 = new Path(args[2]+"ProblemOne");
		conf.setNumMapTasks(2);
		conf.setNumReduceTasks(4);
		j1.setJobName("Stage 1: Section wise station vectors");
		j1.setJarByClass(ProblemOne.class);

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
