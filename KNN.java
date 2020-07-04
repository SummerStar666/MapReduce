import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class KNN {
        static private Vector<Float[]> train = new Vector<Float[]>();

        static void init(){
                Float[] temp = new Float[5];
                Float[] temp1 = new Float[5];
                Float[] temp2 = new Float[5];
                Float[] temp3 = new Float[5];
                Float[] temp4 = new Float[5];
                Float[] temp5 = new Float[5];
                //temp[0]是label，共有三种类别。共有四种不同的属性。
                temp[0] = 0f;temp[1] = 4.9f;temp[2]=3.0f;temp[3]=1.4f;temp[4]=0.2f;train.add(temp);

                temp1[0] = 0f;temp1[1] = 5.4f;temp1[2]=3.7f;temp1[3]=1.5f;temp1[4]=0.2f;train.add(temp1);
                temp2[0] = 1f;temp2[1] = 5.6f;temp2[2]=2.5f;temp2[3]=3.9f;temp2[4]=1.1f;train.add(temp2);
                temp3[0] = 1f;temp3[1] = 6.4f;temp3[2]=2.9f;temp3[3]=4.3f;temp3[4]=1.3f;train.add(temp3);
                temp4[0] = 2f;temp4[1] = 7.6f;temp4[2]=3.0f;temp4[3]=6.6f;temp4[4]=2.1f;train.add(temp4);
                temp5[0] = 2f;temp5[1] = 7.2f;temp5[2]=3.6f;temp5[3]=6.1f;temp5[4]=2.5f;train.add(temp5);
                
        }

        public static class Map extends Mapper<LongWritable, Text, Text, Text> {

                @Override
                protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                        
                        init();

                        String line = value.toString();
                        String[] points = line.split(";");//划分好的样本点

                        Vector<Float[]> sample_data = new Vector<Float[]>();//保存具体的样本点数据

                        //将样本变成数据
                        for(String point : points){
                                String[] attribute = point.split(",");
                                Float[] data = new Float[attribute.length];

                                for(int i = 0; i < attribute.length; i++){
                                        data[i] = Float.parseFloat(attribute[i]);
                                }

                                sample_data.add(data);
                        }

                        //计算距离
                        Float[][] distance = new Float[points.length][train.size()];

                        for(int i = 0; i < points.length; i++){
                                for(int j = 0; j < train.size(); j++){
                                        float dist = 0f;
                                        for(int k = 0; k < 4; k++){
                                                float temp = sample_data.get(i)[k]-train.get(j)[k+1];
                                                dist += (float)Math.pow(temp, 2);
                                        }
                                        distance[i][j] = (float)Math.sqrt((double)dist);
                                }

                        }

                        for(int i = 0; i < points.length; i++){
                                for(int j = 0; j < train.size(); j++)
                                        context.write(new Text(String.valueOf(i)), new Text( train.get(j)[0] + "," + distance[i][j] ));
                        }

                }
        }

        public static class Reduce extends Reducer<Text, Text, Text, Text> {
                @Override
                protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

                        HashMap<Float, Float> map = new HashMap<>();


                        for(Text value : values){

                                String[] temp = value.toString().split(",");

                                Float label = Float.parseFloat(temp[0]);
                                Float dist = Float.parseFloat(temp[1]);

                                if(map.containsKey(dist))
                                        continue;
                                else
                                        map.put(dist, label);

                        }

                        Float mini = 1000f;
                        
                        //选择距离最近的点
                        for(Float k : map.keySet()){
                                if(k < mini) mini = k;
                        }

                        context.write(key, new Text(map.get(mini).toString()));
                }
        }

        public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "knn");
                job.setJarByClass(KNN.class);
                job.setMapperClass(Map.class);
                job.setReducerClass(Reduce.class);
                job.setMapOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.setInputPaths(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                boolean b = job.waitForCompletion(true);

                if(!b) {
                    System.out.println("KNN task fail!");
                }

        }
}
