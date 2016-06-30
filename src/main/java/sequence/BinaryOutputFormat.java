package sequence;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Our BinaryOutputFormat class creates an output stream and sets the
 * BinaryRecordWriter. We could have also used the standard TextOutputFormat class
 * and created the strings in the Mapper or Reducer of our job.
 */
public class BinaryOutputFormat extends FileOutputFormat<Text, Text>
{

	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException
	{
		Path path = getDefaultWorkFile(context, "");
		FileSystem fs = path.getFileSystem(context.getConfiguration());
		FSDataOutputStream output = fs.create(path, context);
		return new BinaryRecordWriter(output, context);
	}

}
