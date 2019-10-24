package io.helidon.examples.sport.graph;

import io.helidon.config.Config;
import io.helidon.messaging.kafka.SimpleKafkaConsumer;
import io.helidon.messaging.kafka.SimpleKafkaProducer;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public class GraphService implements Service {

    private String demoSource;

    private static Logger LOG = Logger.getLogger(GraphService.class.getSimpleName());
    private final SimpleKafkaProducer<Long, String> jobDoneProducer;
    private final Config config;

    GraphService(SimpleKafkaConsumer<Long, String> graphQueueConsumer, SimpleKafkaProducer<Long, String> jobDoneProducer, Config config) {
        this.jobDoneProducer = jobDoneProducer;
        this.config = config;
        this.demoSource = new Scanner(this.getClass().getResourceAsStream("demo.R")).useDelimiter("\\A").next();
        graphQueueConsumer.consumeAsync(r -> generateGraph(r.value()));
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/demo", this::genDemoGraph);
    }

    private void genDemoGraph(ServerRequest serverRequest, ServerResponse serverResponse) {
        String demoData = new Scanner(this.getClass().getResourceAsStream("Afternoon_Run.json")).useDelimiter("\\A").next();
        LOG.info("Preparing R context");
        Context.Builder builder = Context.newBuilder().allowAllAccess(true);
        AtomicReference<Context> ctx = new AtomicReference<>(builder.build());
        Value fn = ctx.get().eval("R", demoSource);
        serverResponse.send(fn.execute(demoData).asString());
    }

    private void generateGraph(String json) {
        LOG.info("Plotting new graph");
        Instant start = Instant.now();
        Context.Builder builder = Context.newBuilder().allowAllAccess(true);
        AtomicReference<Context> ctx = new AtomicReference<>(builder.build());
        Value fn = ctx.get().eval("R", demoSource);
        String resultSvg = fn.execute(json).asString();
        LOG.info(MessageFormat.format("Graph finished in {0} sending back",
                Duration.between(start, Instant.now()).toString()
                        .replaceAll("(PT)?(\\d+[.]?\\d*[HMS])", "$2 ")
                        .toLowerCase()));
        jobDoneProducer.produce(resultSvg);
    }

}
