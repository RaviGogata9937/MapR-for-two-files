# MapR-for-two-files

package org.Wordcount;
import java.io.IOException;
import java.util.Arrays;        //Mapper2 class

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//Will Process
/*
1,joe,75000,developer
2,jack,76000,developer
3,jim,89000,manager
4,jill,99000,director
5,chris,88000,developer
6,ryan,92000,manager
7,tom,77000,admin
8,tim,88000 developer
9,john,56000,developer
10,james,78000,developer
*/
public class MyMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
 
 public void map(LongWritable offset, Text value, Context con) throws IOException,
   InterruptedException {
  System.out.println("MyMapper2.map(-,-,-)");
  String line = value.toString();
  String[] words = line.split(",");
  System.out.println(Arrays.toString(words));
  if (words[3].equals("developer")){
  String name = words[0] +"," +words[1];
  int salary= Integer.parseInt(words[2]);
   System.out.println(name+"::"+salary);
  con.write(new Text(name), new IntWritable(salary));
 }

 }
}
package org.Wordcount;


import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
 //Mapper 1 class
 /*Will Process
1,joe
4,jill
5,chris
6,ryan
8,tim
7,tom
9,john
10,james
*/
 
 public void map(LongWritable offset, Text value, Context context) throws IOException,
   InterruptedException {
  System.out.println("MyMapper1.map(-,-)");
  String line = value.toString();
  String[] words = line.split(",");
  System.out.println(Arrays.toString(words));
  String name = words[0]+","+ words[1];
  int salary=0; 
  System.out.println(name+"::"+salary);
  context.write(new Text(name), new IntWritable(salary));
 }
}
package org.Wordcount;
//Driver class
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MyDriver {
 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
  
  Path input_dir1=new Path("hdfs://localhost:9000/user/skillvoice/employee.txt");
  Path input_dir2=new Path("hdfs://localhost:9000/user/skillvoice/employee_reference.txt");
  
     Path output_dir=new Path("hdfs://localhost:9000/output_data");
  
  /*Reads hadoop configuration file,and points to the hadoop cluster*/
  Configuration conf = new Configuration();
  
  //Create an object of Job by specifying conf object
  Job job = new Job(conf, "MyWordCountJob");
  
  
  
  //Set your main class in the jar file that will be created in future
     job.setJarByClass(MyDriver.class);
  
     job.setMapperClass(MyMapper2.class);
     job.setMapperClass(MyMapper1.class);
    
     job.setReducerClass(MyReducer.class);
    
     job.setMapOutputKeyClass(Text.class);
     job.setMapOutputValueClass(IntWritable.class);

    
     MultipleInputs.addInputPath(job, input_dir1, TextInputFormat.class, MyMapper2.class);
     //empList2.txt will be proceded by MyMapper1
     MultipleInputs.addInputPath(job, input_dir2, TextInputFormat.class, MyMapper1.class);
    
     FileOutputFormat.setOutputPath(job,output_dir );
     output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,true);
  
     //This piece of code will actually intiate the Job run
     System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}
package org.Wordcount;
import java.io.IOException;
//Reducer class
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

 public void reduce(Text name, Iterable<IntWritable> salaries,
   Context context) throws IOException, InterruptedException {
  System.out.println("MyReducer.reducer()"+ name);
  System.out.println("Reducer is working");
  int total = 0;
  for (IntWritable salary : salaries) {
   total += salary.get();
   System.out.println(total);
  }
  context.write(name, new IntWritable(total));
 }
}
//POM.xml
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
	<modelVersion>4.0.0</modelVersion>
	<groupId>WordCountCDH4</groupId>
	<artifactId>WordCountCDH4</artifactId>
	<version>0.0.1-SNAPSHOT</version>
	<build>
		<sourceDirectory>src</sourceDirectory>
		<plugins>
			<plugin>
				<artifactId>maven-compiler-plugin</artifactId>
				<version>3.3</version>
				<configuration>
					<source>1.7</source>
					<target>1.7</target>
				</configuration>
			</plugin>
		</plugins>
	</build>
	<dependencies>
		<dependency>
    <groupId>jdk.tools</groupId>
    <artifactId>jdk.tools</artifactId>
    <version>1.8.0_73</version>
    <scope>system</scope>
    <systemPath>C:/Program Files/Java/jdk1.8.0_73/lib/tools.jar</systemPath>
</dependency>
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-core</artifactId>
			<version>1.2.1</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-common</artifactId>
			<version>0.23.6</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-client</artifactId>
			<version>1.2.1</version>
		</dependency>
		<dependency>
			<groupId>org.apache.pig</groupId>
			<artifactId>pig</artifactId>
			<version>0.11.0</version>
		</dependency>
		<dependency>
			<groupId>org.codehaus.jackson</groupId>
			<artifactId>jackson-core-asl</artifactId>
			<version>1.7.3</version>
		</dependency>
		<dependency>
			<groupId>org.apache.commons</groupId>
			<artifactId>commons-math3</artifactId>
			<version>3.2</version>
		</dependency>
		<dependency>
			<groupId>junit</groupId>
			<artifactId>junit</artifactId>
			<version>4.8.1</version>
			<scope>test</scope>
		</dependency>
		<dependency>
			<groupId>org.antlr</groupId>
			<artifactId>antlr</artifactId>
			<version>3.4</version>
			<scope>test</scope>
		</dependency>
		<dependency>
			<groupId>log4j</groupId>
			<artifactId>log4j</artifactId>
			<version>1.2.16</version>
			<scope>test</scope>
		</dependency>
		<dependency>
			<groupId>joda-time</groupId>
			<artifactId>joda-time</artifactId>
			<version>2.0</version>
			<scope>test</scope>
		</dependency>
		<!-- MRUnit Maven Repository -->
		<dependency>
			<groupId>org.apache.mrunit</groupId>
			<artifactId>mrunit</artifactId>
			<version>0.9.0-incubating</version>
			<classifier>hadoop1</classifier>
		</dependency>
		<!-- Oozie Maven Configurations. -->
		<dependency>
			<groupId>org.apache.oozie</groupId>
			<artifactId>oozie-client</artifactId>
			<version>4.2.0</version>
		</dependency>
	</dependencies>
</project>
