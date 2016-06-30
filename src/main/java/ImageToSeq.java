import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Created by cloudera on 6/29/16.
 */
public class ImageToSeq {
    public static void main(String args[]) throws Exception {

        Configuration confHadoop = new Configuration();
        confHadoop.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/core-site.xml"));
        confHadoop.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(confHadoop);
        Path inPath = new Path("/mapin/1.png");
        Path outPath = new Path("/mapin/11.png");
        FSDataInputStream in = null;
        Text key = new Text();
        BytesWritable value = new BytesWritable();
        SequenceFile.Writer writer = null;
        try{
            in = fs.open(inPath);
            byte buffer[] = new byte[in.available()];
            in.read(buffer);
            writer = SequenceFile.createWriter(fs, confHadoop, outPath, key.getClass(),value.getClass());
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