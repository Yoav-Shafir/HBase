package tablesAsDataSink;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

// ImportFromFile MapReduce job that reads from a file and writes into a table.
public class ImportFromFile {
	private static final Log LOG = LogFactory.getLog(ImportFromFile.class);

	// Define a job name for later use.
	public static final String NAME = "ImportFromFile";

	public enum Counters {
		LINES
	}

	// // Define the mapper class, extending the provided Hadoop class.
	static class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Mutation> {
		private byte[] family = null;
		private byte[] qualifier = null;

		/**
		 * called once when the class is instantiated by the framework. Here it
		 * is used to parse the given column into a column family and qualifier
		 *
		 * @param context
		 *            The task context.
		 * @throws IOException
		 *             When an operation fails - not possible here.
		 * @throws InterruptedException
		 *             When the task is aborted.
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			String column = context.getConfiguration().get("conf.column");

			// Splits a column in family:qualifier form into separate byte
			// arrays.
			byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
			family = colkey[0];
			if (colkey.length > 1) {
				qualifier = colkey[1];
			}
		}

		/**
		 * being called for every row in the input text file, each containing a
		 * JSON record. The map() function transforms the key/value provided by
		 * the InputFormat to what is needed by the OutputFormat.
		 *
		 * @param offset
		 *            The current offset into the input file.
		 * @param line
		 *            The current line of the file.
		 * @param context
		 *            The task context.
		 * @throws IOException
		 *             When mapping the input fails.
		 */
		@Override
		protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
			try {
				String lineString = line.toString();

				// creates a HBase row key by using an MD5 hash of the line
				// content. It then stores the line content as-is in the
				// provided column, ti‐
				// tled data:json
				// byte[] rowkey = DigestUtils.md5(lineString);
				byte[] rowkey = Bytes.toBytes("rowKey1");

				// Create a Put operation for the specified row.
				// row - row key
				Put put = new Put(rowkey);
				put.addColumn(family, qualifier, Bytes.toBytes(lineString));

				// Store the original data in a column in the given table.
				// ImmutableBytesWritable - A byte sequence that is usable as a
				// key or value.
				context.write(new ImmutableBytesWritable(rowkey), put);
				context.getCounter(Counters.LINES).increment(1);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Parse the command line parameters using the Apache Commons CLI classes.
	 * These are already part of HBase and therefore are handy to process the
	 * job specific parameters.
	 *
	 * @param args
	 *            The parameters to parse.
	 * @return The parsed command line.
	 * @throws ParseException
	 *             When the parsing of the parameters fails.
	 */
	private static CommandLine parseArgs(String[] args) throws ParseException {
		Options options = new Options();
		Option o = new Option("t", "table", true, "table to import into (must exist)");
		o.setArgName("table-name");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("c", "column", true, "column to store row data into (must exist)");
		o.setArgName("family:qualifier");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("i", "input", true, "the directory or file to read from");
		o.setArgName("path-in-HDFS");
		o.setRequired(true);
		options.addOption(o);

		options.addOption("d", "debug", false, "switch on DEBUG log level");

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (Exception e) {
			System.err.println("ERROR: " + e.getMessage() + "\n");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(NAME + " ", options, true);
			System.exit(-1);
		}
		return cmd;
	}

	/**
	 * Main entry point.
	 *
	 * @param args
	 *            The command line parameters.
	 * @throws Exception
	 *             When running the job fails.
	 */
	public static void main(java.lang.String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();

		// GenericOptionsParser is a utility to parse command line arguments
		// generic to the Hadoop framework
		// Give the command line arguments to the generic parser first to handle
		// "-Dxyz" properties.
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		CommandLine cmd = parseArgs(otherArgs);

		// check debug flag and other options
		if (cmd.hasOption("d"))
			conf.set("conf.debug", "true");

		// get details
		String table = cmd.getOptionValue("t");
		String input = cmd.getOptionValue("i");
		String column = cmd.getOptionValue("c");
		conf.set("conf.column", column);

		// Define the job with the required classes.
		Job job = Job.getInstance(conf, "Import from file " + input + " into table " + table);
		job.setJarByClass(ImportFromFile.class);
		job.setMapperClass(ImportMapper.class);

		// provided by HBase and allows the job to easily write data into a
		// table
		// The key and value types needed by this class are implicitly fixed to
		// ImmutableBytesWritable for the key, and Mutation for the value
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Writable.class);

		// his is a map only job, therefore tell the framework to bypass the
		// reduce step.
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(input));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
