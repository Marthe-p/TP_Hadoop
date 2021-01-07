import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Obj3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text City = new Text();

    public void map(LongWritable Key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String InputLine = value.toString();
        String[] Splited_InputLine = InputLine.split("\t");

        if (Splited_InputLine[3] == "Children's Clothing") {
            City.set(Splited_InputLine[2]);
            context.write(City, one);
        }

    }
}
