package org;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * The InputFormat class is extending CombineFileInputFormat to efficiently read
 * small files from HDFS. It then sets our BinaryRecordReader that will extract
 * the text from the documents.
 */
public class TikaInputFormat extends FileInputFormat<Text, Text>
{
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException
	{

		//return new BinaryRecordReader((CombineFileSplit) split, context);
		return new TikaRecordReader((FileSplit) split, context);
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file)
	{
		return false;
	}
}
