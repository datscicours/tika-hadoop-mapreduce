package org;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * This is the main conversion class. It gets a list of documents in a split and
 * then reads them one by one returning a record with the filename as the key
 * and the file content as the value.
 */
public class TikaRecordReader extends RecordReader<Text, Text>
{

	private FileSplit split;
	private FileSystem fs;
	private Text key, value;
	private Path paths;
	private FSDataInputStream currentStream;
	private TarArchiveInputStream tarStream;
	private TikaHelper tikaHelper;
	private Configuration conf;

	// count and done are used for progress
	private int count = 0;
	private boolean done = false;
	private boolean complete = true;

	public TikaRecordReader(FileSplit split, TaskAttemptContext context)
			throws IOException
	{
		this.paths = split.getPath();
		this.conf = context.getConfiguration();
		this.fs = FileSystem.get(conf);
		this.split = split;
		this.tikaHelper = new TikaHelper(context.getConfiguration());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{

		TarArchiveEntry entry;

		while((entry = tarStream.getNextTarEntry()) != null){
			if(entry.isFile())
				break;
		}

		if(entry != null){
			key.set(entry.getName());
			value.set(tikaHelper.getMetadata(tarStream));
			count++;

			return true;
		}
		else
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
	public Text getCurrentValue() throws IOException, InterruptedException
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

		key = new Text();
		value = new Text();

		this.fs = paths.getFileSystem(conf);
		currentStream = fs.open(paths);

		tarStream = new TarArchiveInputStream(currentStream);

	}
}
