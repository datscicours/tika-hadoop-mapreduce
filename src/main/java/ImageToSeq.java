import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Created by cloudera on 6/29/16.
 */
public class ImageToSeq {
    public static void main(String args[]) throws Exception {

        Configuration confHadoop = new Configuration();
        confHadoop.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        confHadoop.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(confHadoop);
        Path inPath = new Path(args[1]);
        Path outPath = new Path(args[2]);
        FSDataInputStream in = null;
        Text key = new Text();
        BytesWritable value = new BytesWritable();
        SequenceFile.Writer writer = null;

        in = inPath.getFileSystem(confHadoop).open(inPath);
        writer = SequenceFile.createWriter(confHadoop,
                SequenceFile.Writer.file(outPath),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(BytesWritable.class));
        try{

            byte[] buffer = new byte[100240];

            //SequenceFile.ValueBytes bytes = new SequenceFile.ValueBytes();
            key = new Text(inPath.getName());
            for (int cbread=0; (cbread = in.read(buffer))>= 0;) {
                writer.appendRaw(key, new BytesWritable(buffer));
            }
            //writer = SequenceFile.createWriter(fs, confHadoop, outPath, key.getClass(),value.getClass());

            writer.append(new Text(inPath.getName()), new BytesWritable(buffer));
        }catch (Exception e) {
            System.out.println("Exception MESSAGES = "+e.getMessage());
        }
        finally {
            IOUtils.closeStream(writer);
            System.out.println("last line of the code....!!!!!!!!!!");
        }
    }
}