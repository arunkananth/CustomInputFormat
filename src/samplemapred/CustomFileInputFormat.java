package samplemapred;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import samplemapred.CustomLineRecordReader;


/**
 * @author hdusr
 *
 */
public class CustomFileInputFormat extends FileInputFormat<LongWritable,Text>{
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(org.apache.hadoop.mapreduce.InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {		
		return new CustomLineRecordReader();
	}
}