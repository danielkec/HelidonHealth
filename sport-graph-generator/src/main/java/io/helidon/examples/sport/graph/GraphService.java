package io.helidon.examples.sport.graph;

import io.helidon.config.Config;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public class GraphService implements Service {

    private Context.Builder builder;
    private AtomicReference<Context> atomGraphCtx = new AtomicReference<>();
    private AtomicReference<Context> atomMapCtx = new AtomicReference<>();
    private AtomicReference<Value> demoFn = new AtomicReference<>();
    private final String demoData;
    private String demoSource;

    private static Logger LOG = Logger.getLogger(GraphService.class.getSimpleName());

    GraphService(Config config) {
        this.demoSource = new Scanner(this.getClass().getResourceAsStream("demo.R")).useDelimiter("\\A").next();
        this.demoData = new Scanner(this.getClass().getResourceAsStream("Afternoon_Run.json")).useDelimiter("\\A").next();
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/demo", this::genDemoGraph);
        rules.get("/jsondata", this::getJsonData);
        rules.get("/throw", this::testThrow);
    }

    private void testThrow(ServerRequest serverRequest, ServerResponse serverResponse) {
        throw new RuntimeException("TEST!!");
    }

    private void getJsonData(ServerRequest serverRequest, ServerResponse serverResponse) {
        serverResponse.send(demoData);
    }

    private void genDemoGraph(ServerRequest serverRequest, ServerResponse serverResponse) {
        LOG.info("Preparing R context");
        this.builder = Context.newBuilder().allowAllAccess(true);
        this.atomGraphCtx = new AtomicReference<>(builder.build());
        this.demoFn.set(atomGraphCtx.get().eval("R", demoSource));
        LOG.info("R context up and ready!");
        serverResponse.send(demoFn.get().execute(this.demoData).asString());
    }

}
