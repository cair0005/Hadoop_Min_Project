import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class LanguageModel {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        int threashold;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            threashold = conf.getInt("threshold", 5);
        }

        //map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //this is cool\t20
            //split between n-1 and n
            //key = n-1 words
            //value = nth word + count
            if ((value == null) || (value.toString().trim().length() == 0)) {
                return;
            }
            String line = value.toString().trim();

            //index0 = this is cool
            //index1 = 20
            String[] words_count = line.split("\t");
            String[] words = words_count[0].split("\\s+");
            int count = Integer.valueOf(words_count[words_count.length - 1]);

            if (words_count.length < 2 || count <= threashold) {
                return;
            }

            //output key and value
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i]).append(" ");
            }
            String outputKey = sb.toString().trim();
            String outputValue = words[words.length - 1] + "=" + count;
            if (!(outputKey == null || outputKey.length() < 1)) {
                context.write(new Text(outputKey), new Text(outputValue));
            }

        }
    }

    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        int n;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            n = conf.getInt("n", 5);
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //this -> <is = 1000, is book = 10>
            TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
            for (Text val : values) {
                String cur_val = val.toString().trim();
                String word = cur_val.split("=")[0].trim();
                int count = Integer.valueOf(cur_val.split("=")[1].trim());
                if(tm.containsKey(count)) {
                    tm.get(count).add(word);
                } else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    tm.put(count,list);
                }
            }

            Iterator<Integer> iter = tm.keySet().iterator();

            for(int j = 0; iter.hasNext() && j < n;) {
                int keyCount = iter.next();
                List<String> words = tm.get(keyCount);
                for(String curWord : words) {
                    context.write(new DBOutputWritable(key.toString(), curWord, keyCount), NullWritable.get());
                    j++;
                    //String temp = key.toString() + " " + curWord + " " + keyCount;
                    //context.getCounter("KeyValue", temp);
                }
            }
        }
    }
}
