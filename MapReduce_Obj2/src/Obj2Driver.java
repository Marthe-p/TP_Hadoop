import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Obj2Driver {
    public static void main(String[] args) throws Exception {
        Configuration Conf = new Configuration();
        Job Traitement = Job.getInstance(Conf, "Total Ventes par Pays");
        Traitement.setJarByClass(Obj2Driver.class);
        Traitement.setMapperClass(Obj2Mapper.class);
        Traitement.setCombinerClass(Obj2Reducer.class);
        Traitement.setReducerClass(Obj2Reducer.class);
        Traitement.setOutputKeyClass(Text.class);
        Traitement.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(Traitement, new Path("storage/TP/purchases.txt"));

        FileOutputFormat.setOutputPath(Traitement, new Path("storage/TP/Obj2_Resultats_Traitement_1"));

        System.exit(Traitement.waitForCompletion(true) ? 0 : 1);
    }
}
