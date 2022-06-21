package io.github.immno.jet.rocketmq;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;

public class RocketmqTest {
    private static HazelcastInstance hz;

    @BeforeClass
    public static void beforeClass() {
        hz = Hazelcast.bootstrappedInstance();
    }

    @AfterClass
    public static void afterClass() {
        hz.shutdown();
    }

    // generate： timestamp表示数据发送的时间戳, sequence表示数据的序号
    @Test(expected = TimeoutException.class)
    public void tesRun() throws InterruptedException, ExecutionException, TimeoutException {
        Pipeline p = Pipeline.create();
        Properties properties = new Properties();
        properties.setProperty(RocketmqConfig.NAME_SERVER_ADDR, "10.75.8.151:9876");
        properties.setProperty(RocketmqConfig.PRODUCER_GROUP, "Mno_jet_test");
        properties.setProperty(RocketmqConfig.ACCESS_CHANNEL, AccessChannel.CLOUD.name());
        p.readFrom(
                RocketmqSources.rocketmq(properties, t -> new String(t.getBody(), StandardCharsets.UTF_8), "mno_test"))
                .withIngestionTimestamps()
                .writeTo(Sinks.logger());
        JetService jet = hz.getJet();
        jet.newJob(p).getFuture().get(100, TimeUnit.SECONDS);
    }

    private FunctionEx<MessageExt, String> extracted() {
        return new FunctionEx<MessageExt, String>() {

            @Override
            public String applyEx(MessageExt t) throws Exception {
                return new String(t.getBody(), StandardCharsets.UTF_8);
            }

        };
    }

}
