package com.arun.top;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class JoinReducer extends Reducer<Text, Text, Text, Text> {
private ArrayList<Text> listMovies = new ArrayList<Text>();
private ArrayList<Text> listRating = new ArrayList<Text>();
private Text outvalue = new Text();
@Override
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
listMovies.clear();
listRating.clear();
for (Text text : values) {
if (text.charAt(0) == 'M') {
listMovies.add(new Text(text.toString().substring(1)));
} else if (text.charAt(0) == 'R') {
listRating.add(new Text(text.toString().substring(1)));
}
}
executeJoinLogic(context);
}
private void executeJoinLogic(Context context) throws IOException, InterruptedException {
double sum = 0;
if (!listMovies.isEmpty() && !listRating.isEmpty()) {
	for (Text moviesData : listMovies) {
	for (Text ratingData : listRating) {
	sum = sum + Double.parseDouble(ratingData.toString());
}
if (listRating.size() > 40) {
	double average = sum / listRating.size();
	outvalue.set(String.valueOf(average));
	context.write(moviesData, outvalue);
}
}
}
}
}