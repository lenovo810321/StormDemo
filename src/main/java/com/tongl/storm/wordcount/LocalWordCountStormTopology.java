package com.tongl.storm.wordcount;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 单词统计
 * Created by 张宝玉 on 2018/10/26.
 */
public class LocalWordCountStormTopology {
    public static class DataSourceSpot extends BaseRichSpout {

        private SpoutOutputCollector collector;
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 业务
         * 1）读取指定目录的文件夹下的数据：E:\examples\StormDemo\data\wc
         * 2）把每一行数据发送出去
         */
        public void nextTuple() {
            //获取所有文件
            Collection<File> files = FileUtils.listFiles(new File("E:\\examples\\StormDemo\\data\\wc"),
                    new String[]{"txt"},true);

            for (File file : files) {
                try {
                    //获取文件中所有内容
                    List<String> lines = FileUtils.readLines(file);
                    for (String line : lines) {
                        //发射出去
                        this.collector.emit(new Values(line));
                    }
                    FileUtils.moveFile(file, new File(file.getAbsolutePath() + System.currentTimeMillis()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }

    /**
     * 对数据分割
     */
    public static class SplitBolt extends BaseRichBolt {

        private OutputCollector collector;
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 业务逻辑
         * line:对line进行分割，按照逗号
         */
        public void execute(Tuple input) {
            String line = input.getStringByField("line");
            String[] words = line.split(" ");

            for (String word : words) {
                this.collector.emit(new Values(word));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class CountBolt extends BaseRichBolt{
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

        }

        Map<String, Integer> map = new HashMap<String, Integer>();
        /**
         * 业务逻辑
         * 1）获取每个单词
         * 2）对所有单词进行汇总
         * 3）输出
         * @param input
         */
        public void execute(Tuple input) {
            // 1)获取单词
            String word = input.getStringByField("word");
            Integer count = map.get(word);
            if(count == null){
                count = 0;
            }
            ++count;

            // 2）对所有单词汇总
            map.put(word,count);

            // 3）输出
            System.out.println("======================");
            Set<Map.Entry<String, Integer>> entrySet = map.entrySet();
            for ( Map.Entry<String, Integer> entry : entrySet) {
                System.out.println(entry);
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {
        //通过TopologyBuilder根据Spou和Bolt构建Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpot",new DataSourceSpot());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSourceSpot");
        builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");

        //创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountStormTopology",new Config(),builder.createTopology());
    }
}
