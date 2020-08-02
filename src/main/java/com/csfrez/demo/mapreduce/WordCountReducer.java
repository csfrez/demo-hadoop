package com.csfrez.demo.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 四个泛型
 * KEYIN: K2类型
 * VALUEIN: V2类型
 *
 * KEYOUT: K3类型
 * VALUEOUT: V3类型
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    /**
     * reduce方法作用：将新的K2和V2转为K3和V3，将K3和V3写入上下文中
     * @param key 新K2
     * @param values 集合 新V2
     * @param context 表示上下文对象
     * @throws IOException
     * @throws InterruptedException
     * 如何将新的K2和V2转为K3和V3
     * 新   K2     V2
     *      hello  <1,1,1>
     *      world  <1,1>
     *      hadoop <1>
     * ----------------------
     *      K3     V3
     *      hello  3
     *      world  2
     *      hadoop 1
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        //1、遍历集合，将集合中的数字相加，得到V3
        for(LongWritable value : values){
            count += value.get();
        }
        //2、将K3和V3写入上下文中
        context.write(key, new LongWritable(count));
    }
}
