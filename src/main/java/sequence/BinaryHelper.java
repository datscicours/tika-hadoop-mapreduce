package sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

/**
 * BinaryHelper class is a Wrapper around the Tika framework To read a file from
 * an input stream replace delimited and new line characters and return the
 * string content.
 */
public class BinaryHelper
{

	// the character used as delimiter, has to be in Regex syntax
	public String delimiter = "[|]";
	// the endLine Character, has to be in Regex syntax
	public String endLine = "[\r\n]";
	// the character used to replace the delimiter and endLine
	public String replaceWith = " ";
	// the instance of the Tika engine
	public Tika tika;
	// the logger instance
	private Logger logger = Logger.getLogger(this.getClass());

	public BinaryHelper(Configuration conf)
	{
		tika = new Tika();
		String confDelimiter = conf.get("com.ibm.imte.tika.delimiter");
		String confReplaceChar = conf
				.get("com.ibm.imte.tika.replaceCharacterWith");
		if (confDelimiter != null)
			this.delimiter = "[" + confDelimiter + "]";
		if (confReplaceChar != null)
			this.replaceWith = confReplaceChar;
		logger.info("Delimiter: " + delimiter);
		logger.info("Replace With character:" + replaceWith);
	}

	public String readPath(InputStream stream)
	{
		try
		{
			String content = tika.parseToString(stream);
			content = content.replaceAll(delimiter, replaceWith);
			content = content.replaceAll(endLine, replaceWith);
			return content;
		} catch (Exception e)
		{
			logger.error("Malformed PDF for Tika: " + e.getMessage());
		}
		return "Malformed PDF";
	}

	public String getMetadata(InputStream stream){

		AutoDetectParser parser = new AutoDetectParser();
		BodyContentHandler handler = new BodyContentHandler();
		Metadata metadata = new Metadata();
		try {
			parser.parse(stream, handler, metadata);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (TikaException e) {
			e.printStackTrace();
		}

		return metadata.toString();

	}

}
