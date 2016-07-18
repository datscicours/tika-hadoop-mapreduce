package sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This is the main conversion class. It gets a list of documents in a split and
 * then reads them one by one returning a record with the filename as the key
 * and the file content as the value.
 */
public class BinaryRecordReader extends RecordReader<Text, BytesWritable>
{

	private FileSplit split;
	private FileSystem fs;
	private Text key;
	private BytesWritable value;
	private Path paths;
	private FSDataInputStream currentStream;
	private BinaryHelper tikaHelper;
	private Configuration conf;

	// count and done are used for progress
	private int count = 0;
	private boolean done = false;

	public BinaryRecordReader(FileSplit split, TaskAttemptContext context)
			throws IOException
	{
		this.paths = split.getPath();
		this.conf = context.getConfiguration();
		this.fs = FileSystem.get(conf);
		this.split = split;
		this.tikaHelper = new BinaryHelper(context.getConfiguration());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{
//		if (count >= split.getNumPaths())
//		{
//			done = true;
//			return false; // we have no more data to parse
//		}

		//Path path = null;
		key = new Text();
		value = new BytesWritable();

//		try
//		{
//			path = this.paths[count];
//		} catch (Exception e)
//		{
//			return false;
//		}

		currentStream = null;
//		TikaInputStream.get(path
		this.fs = paths.getFileSystem(conf);
		//this.fs = FileSystem.get(new URI(paths.getName()), conf);
		currentStream = fs.open(paths);

		key.set(paths.getName());
		ByteBuffer buff = ByteBuffer.allocate(4096);
		currentStream.read(buff);
		//value.set(new BytesWritable(buff.array()));

		currentStream.close();
		count++;

		//return true; // we have more data to parse
		return false;
	}

	@Override
	public void close() throws IOException
	{
		done = true;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException
	{
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException
	{
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException
	{
		//return done ? 1.0f : (float) (count / paths.length);
		return 1;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException
	{
	}
}
