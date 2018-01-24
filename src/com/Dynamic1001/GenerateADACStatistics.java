package com.Dynamic1001;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.Dynamic1001.GenerateADACStatistics.MyMap.MyReducer;

public class GenerateADACStatistics {
	public static class MyMap extends TableMapper<Text, IntWritable> {
		private final IntWritable ONE = new IntWritable(1);

		public void map(ImmutableBytesWritable row, Result result, Context context) throws IOException, InterruptedException {

			String referrer = new String(result.getValue(Bytes.toBytes("T"), "Referrer".getBytes()));
			String CampaignId = new String(result.getValue(Bytes.toBytes("T"), "CampaignId".getBytes()));
			String TS = new String(result.getValue(Bytes.toBytes("T"), "Ts".getBytes()));
			String Ignore = new String(result.getValue(Bytes.toBytes("T"), "Ignore".getBytes()));
			if (Ignore.equalsIgnoreCase("false")) {

				try {
					String date = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(TS));
					int Hour = Integer.parseInt(new SimpleDateFormat("HH").format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(TS)));
					StringBuilder FinalData = new StringBuilder();
					FinalData.append(CampaignId).append("\001");// 0
					FinalData.append(date).append("\001");// 1
					FinalData.append(new String(result.getValue(Bytes.toBytes("T"), "ActionType".getBytes()))).append("\001");// 2
					FinalData.append(parseReferrer(referrer)).append("\001");// 3
					FinalData.append(Hour).append("\001");// 3
					context.write(new Text(FinalData.toString()), ONE);

				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		public String parseReferrer(String referrer) {
			if (!referrer.isEmpty()) {
				if (referrer.contains("?")) {
					return (referrer.substring(0, referrer.indexOf("?")));
				} else
					return referrer;
			} else
				return "";

		}

		public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

			public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
				int i = 0;
				for (IntWritable val : values) {
					i += val.get();
				}
				String[] Data = key.toString().split("\001");
				StringBuilder finalData = new StringBuilder();
				int unique_clicks = 0, uniqueviews = 0;
				if (Data[2].equals("1")) {
					uniqueviews = i;
				} else if (Data[2].equals("10")) {
					unique_clicks = i;
				}
				finalData.append(Data[1]).append("\001");// Date
				finalData.append(Data[4]).append("\001");// Hour
				finalData.append(Data[0]).append("\001");// Campaign ID
				finalData.append(Data[3]).append("\001");// Page
				finalData.append(uniqueviews).append("\001");
				finalData.append(unique_clicks).append("\001");
				context.write(new Text(finalData.toString()), null);
			}
		}
	}

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		String InputTableName = args[0];
		String date = args[1];
		String OutputDir = args[2];

		String end_Date = "";
		if (date.contains("-") || end_Date.contains("-")) {
			date = date.replace("-", "");
			end_Date = date.replace("-", "");
		}
		String Start_Day_Key = date + "_12051_0*"; // In Format
													// like
													// 20140712_1*
		String End_Day_Key = end_Date + "_12051_zzzzzz*";// In
															// format
		// Like
		// 20140712_9*

		System.out.println("Star Region Scan Key :: " + Start_Day_Key);
		System.out.println("Star Region Scan Key :: " + End_Day_Key);

		Configuration config = HBaseConfiguration.create();
		config.set("fs.default.name", "hdfs://hnn001");
		config.set("hbase.rootdir", "hdfs://hnn001:8020/hbase");
		config.setInt("hbase.client.scanner.timeout.period", 300000);

		config.set("fs.default.name", "hdfs://hnn001");
		config.set("hbase.rootdir", "hdfs://hnn001:8020/hbase");
		config.set("hbase.client.scanner.timeout.period", "300000");
		config.set("zookeeper.session.timeout", "1200000");
		config.set("hbase.zookeeper.quorum", "HDN001,HDN002,HNN001,HDN003");
		config.set("hbase.regionserver.lease.period", "1800000");
		config.set("hbase.rpc.timeout", "300000");
		config.set("mapred.reduce.slowstart.completed.maps", "1.0");

		Job job = new Job(config, "ADAC Page Url Statistics For Date " + date);
		job.setJarByClass(GenerateADACStatistics.class); // class that
		// contains
		// mapper and
		// reducer

		Scan scan = new Scan();
		scan.setCaching(1000);
		scan.setStartRow(Start_Day_Key.getBytes());
		scan.setStopRow(End_Day_Key.getBytes());
		scan.addColumn("T".getBytes(), "Referrer".getBytes());
		scan.addColumn("T".getBytes(), "Ts".getBytes());
		scan.addColumn("T".getBytes(), "ActionType".getBytes());
		scan.addColumn("T".getBytes(), "CampaignId".getBytes());
		scan.addColumn("T".getBytes(), "Ignore".getBytes());
		FilterList list_actiontype_filter = new FilterList(Operator.MUST_PASS_ONE);
		list_actiontype_filter.addFilter(new SingleColumnValueFilter(Bytes.toBytes("T"), Bytes.toBytes("ActionType"), CompareOp.EQUAL, Bytes.toBytes("1")));
		list_actiontype_filter.addFilter(new SingleColumnValueFilter(Bytes.toBytes("T"), Bytes.toBytes("ActionType"), CompareOp.EQUAL, Bytes.toBytes("10")));
		list_actiontype_filter.addFilter(new SingleColumnValueFilter(Bytes.toBytes("T"), Bytes.toBytes("Ignore"), CompareOp.EQUAL, Bytes.toBytes("false")));
		scan.setFilter(list_actiontype_filter); // Scans only those data which
		// satisfy the filter condition
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		TableMapReduceUtil.initTableMapperJob(InputTableName, // input table
				scan, // Scan instance to control CF and attribute selection
				GenerateADACStatistics.MyMap.class, // mapper class
				Text.class, // mapper output key
				IntWritable.class, // mapper output value
				job);

		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1); // at least one, adjust as required

		// job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(OutputDir));
		job.submit();
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
}
