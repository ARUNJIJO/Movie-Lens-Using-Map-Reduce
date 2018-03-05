package com.arun.top;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class MoviesMapper extends Mapper<Object, Text, Text, Text> {
private Text movieId = new Text();
private Text outvalue = new Text();
@Override
public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
	String data = values.toString();
	String[] field = data.split("::", -1);
	if (null != field && field.length == 3 && field[0].length() > 0) {
	movieId.set(field[0]);
	outvalue.set("M" + field[1]);
	context.write(movieId, outvalue);
}
}
}