import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCounterDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res= ToolRunner.run(new WordCounterDriver(), args); //Se declara un entero que le declare la funcion ToolRunner
		System.exit(res); 
	}

	public int run(String[] args) throws Exception{

		Job job= Job.getInstance(getConf(), "WordCounter");

		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPath(job, new Path(args[0])); //Se declara el Path de entrada
		FileOutputFormat.setOutputPath(job, new Path(args[1])); //Se declara el Path de salida

		job.setMapperClass(WordCounterMapper.class); //Se selecciona la clase de mapeo
		job.setReducerClass(WordCounterReducer.class); //Se seleciona la clase de reduccion

		job.setOutputKeyClass(Text.class); //Establecemos como clave de salida la palabra
		job.setOutputValueClass(IntWritable.class); //Numero de veces que se repite la clave

		return job.waitForCompletion(true) ? 0: 1;
	}
}

