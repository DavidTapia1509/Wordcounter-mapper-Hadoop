import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private final static Pattern SPLIT_PATTERN = Pattern.compile("\\s*\\b\\s*");
			;
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	 String line= value.toString(); //Inicializamos el String linea
	 line= line.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase(); //Reemplaza todo lo que no sean letras o numeros por nada
	 Text currentWord= new Text();
	 
	 String words[]= SPLIT_PATTERN.split(line); //hacemos un string para guardar las palabras
	 
	 for (int i = 0; i < words.length; i++) { //recorremos el array
		 
		if(words[i].isEmpty()){
			continue;	//Si hay un espacio se continua recorriendo
		}
		currentWord= new Text(words[i]); //Se transforma el string de Java a String de Haddop
		context.write(currentWord, one);
	}  
  }
}
