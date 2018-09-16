package com.cjie.storm.trident.trend.logappender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.cjie.storm.trident.trend.message.MessageFormatter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


/**
 * Created with IntelliJ IDEA.
 * User: hucj
 * Date: 14-6-23
 * Time: 下午8:27
 * To change this template use File | Settings | File Templates.
 */
public class KafkaAppender extends
        AppenderBase<ILoggingEvent> {
    private String topic;
    private String zookeeperHost;
    private Formatter formatter;

    private static KafkaProducer<String, String> producer = null;
    private static Properties props = new Properties();

    // java bean definitions used to inject
    // configuration values from logback.xml
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
    public String getZookeeperHost() {
        return zookeeperHost;
    }
    public void setZookeeperHost(String zookeeperHost)
    {
        this.zookeeperHost = zookeeperHost;
    }
    public Formatter getFormatter() {
        return formatter;
    }
    public void setFormatter(Formatter formatter) {
        this.formatter = formatter;
    }

    private static void initKafka(){
        if(producer==null){
            props.put("bootstrap.servers", "localhost:9092");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            // Starting producer
            producer = new KafkaProducer<>(props);
        }
    }


    // overrides
    @Override
    public void start() {
        if (this.formatter == null) {
            this.formatter = new MessageFormatter();
        }
        super.start();
        initKafka();
    }
    @Override
    public void stop() {
        super.stop();
        this.producer.close();
    }
    @Override
    protected void append(ILoggingEvent event) {
       String payload = this.formatter.format(event);
       ProducerRecord<String, String> data = new ProducerRecord<String, String>(this.topic, payload);
       this.producer.send(data);
    }
    public static void main(String[] args) {
        initKafka();
        String payload = String.format("abc%s","test");
        ProducerRecord<String, String> data = new ProducerRecord<String, String>("mytopic", payload);
        producer.send(data);
    }
}
