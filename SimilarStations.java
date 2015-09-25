import java.io.*;
import java.math.*;
import java.util.regex.*;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SimilarStns
{

	/*
		The Mapper function splits each record and emits the <K, V> pair as <temp, "1">
	*/

	public static class Stage1Mapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	  {
	    	String str = value.toString();
				String[] strList = str.split("  *");
				double temp, dewPoint, windSpeed;
				String newValue;
				String newKey;
				String secID;
				String[] dateList;
				int hour;
				int month;
				try
				{

				  temp = Double.parseDouble(strList[3]);
				//System.out.println("****");

					if ( temp == 9999.9 ) 
						newValue = "X";
					else
						newValue = strList[3];

				newKey = strList[0];

					dewPoint = Double.parseDouble(strList[4]);


					if ( dewPoint == 9999.9 ) 
						newValue = newValue + "|X";
					else
						newValue = newValue + "|" + strList[4];

					windSpeed = Double.parseDouble(strList[12]);
					if ( windSpeed == 999.9 ) 
						newValue = newValue + "|X";
					else
						newValue = newValue + "|" + strList[12];


				dateList = strList[2].split("_");
				hour = Integer.parseInt(dateList[1]);
				if (hour >= 4 && hour < 10)
					secID = "S1";
				else if ( hour >= 10 && hour < 16)
					secID = "S2";
				else if ( hour >= 16 && hour < 22)
					secID = "S3";
				else 
					secID = "S4";
				
				month = ((dateList[0].charAt(4)-48)*10) + (dateList[0].charAt(5)-48);
				
				newKey = strList[0] + "_" + month + "_" + secID;

   			context.write(new Text(newKey), new Text(newValue));
				}
				catch (Exception e)
				{}

    }
  }

	/*
		The reducer function
			Input <K, V> Pair = <temp, {list of "1"}>
			Output <K, V> Pair = <temp, count_of_temp>
	*/
	public static class Stage1Reducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
		    double sumTemp = 0;
				int totTemp = 0;
				double secAvgT = 0;

		    double sumDewPoint = 0;
				int totDewPoint = 0;
				double secAvgDP = 0;

		    double sumWindSpeed = 0;
				int totWindSpeed = 0;
				double secAvgWS = 0;


		    for (Text val : values)
				{
						String str = val.toString();
	 					String[] secVec = str.split("\\|");
					 	if (!secVec[0].equals("X"))
					 	{
							sumTemp += Double.parseDouble(secVec[0]);
							totTemp++;
						}
					 	if (!secVec[1].equals("X"))
					 	{
							sumDewPoint += Double.parseDouble(secVec[1]);
							totDewPoint++;
						}
					 	if (!secVec[2].equals("X"))
					 	{
							sumWindSpeed += Double.parseDouble(secVec[2]);
							totWindSpeed++;
						}
    		}

				if(totTemp != 0)
					secAvgT = Math.floor(sumTemp*10000/totTemp)/10000;
				if(totDewPoint != 0)
					secAvgDP = Math.floor(sumDewPoint*10000/totDewPoint)/10000;
				if(totWindSpeed != 0)
					secAvgWS = Math.floor(sumWindSpeed*10000/totWindSpeed)/10000;
				
				String newValue = secAvgT + "|" + secAvgDP + "|" + secAvgWS;
				
				context.write(key, new Text(newValue));

		}
	}

	public static class Stage2Mapper extends Mapper<Text, Text, Text, Text>
	{
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException
	  {
	    	String str = key.toString();
				String[] strList = str.split("_");

				String newKey = strList[1] + "_" + strList[0];					// Key: <monthID>_<stationID>
		
				String newValue = strList[2] + "_" + value.toString();	// Value: <sectionID>_<avgTemp>|<avgDewPoint>|<avgWindSpeed>
				System.out.println(newKey + " " + newValue);			
   			context.write(new Text(newKey), new Text(newValue));
    }
  }

	/*
		The reducer function
			Input <K, V> Pair = <temp, {list of "1"}>
			Output <K, V> Pair = <temp, count_of_temp>
	*/
	public static class Stage2Reducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String[] secVec = new String[4];
			String str;
			String[] strList;
			int i;
			for (Text val : values)
			{
				str = val.toString();
				strList = str.split("_");
				System.out.println(strList[0].charAt(1)-49);
				secVec[strList[0].charAt(1)-49] = strList[1];					
			}
			
			String newValue = secVec[0];
			for (i = 1; i < 4; i++)
				newValue = newValue + "|" + secVec[i];				// Value: <avgTemp>|<avgDewPoint>|<avgWindSpeed>

 			context.write(key, new Text(newValue));
		}
	}

	public static class Stage3Mapper extends Mapper<Text, Text, Text, Text>
	{

		private long iter, numberOfClusters;
		private	double[][] clusterCentroids;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			iter=context.getConfiguration().getLong("iterationNumber", 0);
			numberOfClusters = 3;	// will be taken as a command line argument

			if (iter > 0 )
			{
	 			clusterCentroids = new double[12*(int)numberOfClusters][12];	// ith row represents the ith centroid
				int monthID, clusterID;
				
				String path;
				Path pt;

				for (monthID = 1; monthID <= 12; monthID++)
				{
					clusterID = 1;
					for (; clusterID <= numberOfClusters; clusterID++)
					{	
						path = "/user/hduser/clusterCentroids/" + monthID + "_" + clusterID;	// files of the form, <monthID>_<clusterID> with centroids
						pt = new Path(path);			
						FileSystem fs = FileSystem.get(new Configuration());
	          BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
	          String centroid = br.readLine();
	          Scanner s = new Scanner(centroid).useDelimiter("\\|");
						for (int i = 0; i < 12; i++)
							clusterCentroids[(monthID-1)*3 + clusterID - 1][i] = Double.parseDouble(s.next());
						br.close();
					}
				}
			}
		}

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException
	  {
	    	String str = key.toString();
				String[] strList = str.split("_");
				int clusterID = 0, i, j, clusterCounter;

				double[] stationStat = new double[12];	// to store the attributes of the current station
				double minDistance = -1, dist;

				if (iter == 0)
				{
					Random randomGenerator = new Random();
		      clusterID = randomGenerator.nextInt((int)numberOfClusters) + 1;
				}
				else
				{
					Scanner s = new Scanner(value.toString()).useDelimiter("\\|");
					for (i = 0; i < 12; i++)
						stationStat[i] = Double.parseDouble(s.next());
					
					i = (Integer.parseInt(strList[0]) - 1) * 3; // the postion in the centroid matrix from where the comparison begins for a given record
					
					// check for predefined numberOfClusters					
					clusterCounter = 1; 					
					for (; clusterCounter <= 3; clusterCounter++)
					{
							dist = 0;
							
							// calculating euclidean distance
							for (j = 0; j < 12; j++)
								dist += Math.pow((stationStat[j] - clusterCentroids[i][j]), 2);
							dist = Math.pow(dist, 0.5);
//							System.out.println("distance in iteration " + iter + " : " + dist + " from clusterID " + clusterCounter);
							if ((minDistance == -1) || (dist < minDistance))	// initialize OR update the minDistance 
							{
								minDistance = dist;
								clusterID = clusterCounter;
							}
							i++;
					}
//					System.out.println("minimum distance in iteration " + iter + " : " + minDistance + ", clusterID : " + clusterID);

				}

				String newKey = strList[0] + "_" + clusterID;					// Key: <monthID>_<clusterID>
		
				String newValue = strList[1] + "_" + value.toString();	// Value: <stationID>_<12 attribute tuple>
//				System.out.println(newKey + " " + newValue);			
   			context.write(new Text(newKey), new Text(newValue));
    }
  }


	/*
		The reducer function
			Input <K, V> Pair = <temp, {list of "1"}>
			Output <K, V> Pair = <temp, count_of_temp>
	*/
	public static class Stage3Reducer extends Reducer<Text, Text, Text, Text>
	{
		private long iter;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			iter=context.getConfiguration().getLong("iterationNumber", 0);
		}

		@Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String[] secVec = new String[4];
			String str;

			String[] strList;
//			String[] stationList;
			
			ArrayList<String> stationList = new ArrayList<String>();
			int i, totalStations = 0;

			double[] newCenter;
			newCenter = new double[12];
			for (i = 0; i < 12; i++)
				newCenter[i] = 0;

			for (Text val : values)
			{
				++totalStations;
				str = val.toString();
				strList = str.split("_");
				stationList.add(strList[0]);
				
				strList = strList[1].split("\\|");


				if (iter == 0)	// initialize random center for a cluster
				{
					for (i = 0; i < 12; i++)
				    newCenter[i] = Double.parseDouble(strList[i]);
					break;
				}
				else
					for (i = 0; i < 12; i++)
				    newCenter[i] += Double.parseDouble(strList[i]);
			}

			for (i = 0; i < 12; i++)
				newCenter[i] = Math.floor(newCenter[i]*10000/totalStations)/10000;
			
			String newValue1 = Double.toString(newCenter[0]);	// new centroid value for a cluster
			for (i = 1; i < 12; i++)
				newValue1 = newValue1 + "|" + newCenter[i];				

			String newValue = newValue1 + "_" + stationList.get(0);

			for (i = 1; i < totalStations; i++)
				newValue = newValue + "," + stationList.get(i);					// Value: <12 attribute new center>_<stationID list>
			
    	String newKey = key.toString() + "_" + totalStations;			// Key: <monthID>_<clusterID>_<totalStations>

		
			String path = "/user/hduser/clusterCentroids/" + key.toString();	// files of the form, <monthID>_<clusterID> with centroids
			Path pt = new Path(path);

//			System.out.println(newValue1);

			// to write to the respective centroid files
			FileSystem fs = FileSystem.get(new Configuration());
			FSDataOutputStream out = fs.create(pt);
			BufferedWriter br=new BufferedWriter(new OutputStreamWriter(out));
      br.write(newValue1);
      br.close();

 			context.write(new Text(newKey), new Text(newValue));
		}
	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			try
			{

//				Path pt = new Path("/user/hduser/params.txt");
//				FileSystem fs = FileSystem.get(new Configuration());

//				if (!fs.exists(pt))
//					InputStream is = fs.open(pt);
//				BufferedReader br=null;
//				br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				
//				br.close();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) throws Exception
	{

		Configuration conf = new Configuration();
		Job j1 = new Job(conf);
		FileSystem fs = FileSystem.get(conf);		
		Path inputPath = new Path(args[0]);
		Path sectionWiseVectors = new Path(args[1] + "_Stage1");
		Path monthWiseVectors = new Path(args[1] + "_Stage2");
		Path clusters = new Path(args[1] + "_Stage3");
		int totalIterations = Integer.parseInt(args[2]);
		j1.setJobName("Stage 1: Section wise station vectors");
		j1.setJarByClass(SimilarStns.class);

		//Mapper input and output
		j1.setMapOutputKeyClass(Text.class);
		j1.setMapOutputValueClass(Text.class);

		//Reducer input and output
		j1.setOutputKeyClass(Text.class);
		j1.setOutputValueClass(Text.class);

		//file input and output of the whole program
		j1.setInputFormatClass(TextInputFormat.class);
		j1.setOutputFormatClass(TextOutputFormat.class);
		
		//Set the mapper class
		j1.setMapperClass(Stage1Mapper.class);

		//set the combiner class for custom combiner
//		j1.setCombinerClass(Stage1Reducer.class);


		//Set the reducer class
		j1.setReducerClass(Stage1Reducer.class);

		//set the number of reducer if it is zero means there is no reducer
//		j1.setNumReduceTasks(2);

		FileInputFormat.addInputPath(j1, inputPath);

		if (fs.exists(sectionWiseVectors))
				fs.delete(sectionWiseVectors, true);			

		FileOutputFormat.setOutputPath(j1, sectionWiseVectors);	

		int code = j1.waitForCompletion(true) ? 1 : 0;

		if (code == 1)
		{
			Job j2 = new Job(conf, "Stage 2: Monthwise Station Vectors");
			j2.setJarByClass(SimilarStns.class);

			//Set the mapper class
			j2.setMapperClass(Stage2Mapper.class);

//			j2.setCombinerClass(Stage1Reducer.class);
			j2.setReducerClass(Stage2Reducer.class);

			// Set the number of reduce tasks to an appropriate number for the amount of data being sorted
//			j2.setNumReduceTasks(2);

			j2.setOutputKeyClass(Text.class);
			j2.setOutputValueClass(Text.class);

			// Set the input to the previous job's output
			j2.setInputFormatClass(KeyValueTextInputFormat.class);
			KeyValueTextInputFormat.setInputPaths(j2, sectionWiseVectors);

			// Set the output path to the command line parameter
			j2.setOutputFormatClass(TextOutputFormat.class);
			if (fs.exists(monthWiseVectors))
				fs.delete(monthWiseVectors, true);			

			FileOutputFormat.setOutputPath(j2, monthWiseVectors);

			// Set the separator to a tab
			//j2.getConfiguration().set("mapred.textoutputformat.separator", "\t");


			// Submit the job
			code = j2.waitForCompletion(true) ? 2 : 0;
		}
	
		if (code == 2)
		{
			
			long iter = 0;			
			while (iter < totalIterations)
			{
				Job j3 = new Job(conf, "Stage 3: Clustering Phase");
				j3.getConfiguration().setLong("iterationNumber", iter);

				j3.setJarByClass(SimilarStns.class);

				//Set the mapper class
				j3.setMapperClass(Stage3Mapper.class);

//			j2.setCombinerClass(Stage1Reducer.class);
				j3.setReducerClass(Stage3Reducer.class);

			// Set the number of reduce tasks to an appropriate number for the amount of data being sorted
//			j2.setNumReduceTasks(2);

				j3.setOutputKeyClass(Text.class);
				j3.setOutputValueClass(Text.class);

				// Set the input to the previous job's output
				j3.setInputFormatClass(KeyValueTextInputFormat.class);
				KeyValueTextInputFormat.setInputPaths(j3, monthWiseVectors);
	
				// Set the output path to the command line parameter
				j3.setOutputFormatClass(TextOutputFormat.class);
				if (fs.exists(clusters))
					fs.delete(clusters, true);			
	
				FileOutputFormat.setOutputPath(j3, clusters);
	
				// Set the separator to a tab
				//j2.getConfiguration().set("mapred.textoutputformat.separator", "\t");
	
	
				// Submit the job
				code = j3.waitForCompletion(true) ? 3 : 0;
				if (code == 3)
					iter++;
				else 
					break;
			}
		}

		System.exit(code);
 	}
}
