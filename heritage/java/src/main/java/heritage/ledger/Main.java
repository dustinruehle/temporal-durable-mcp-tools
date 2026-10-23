package heritage.ledger;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One process: the mock ledger HTTP API on port 9103 and a Temporal Worker for the ledger Activities on
 * the {@code heritage-ledger} Task Queue.
 *
 * <p>Environment: {@code TEMPORAL_ADDRESS} (default localhost:7233), {@code TEMPORAL_NAMESPACE} (default
 * default), {@code TEMPORAL_API_KEY} (if set: TLS and API key auth), {@code HERITAGE_JAVA_URL} (default
 * http://localhost:9103), {@code HERITAGE_JAVA_PORT} (default 9103), {@code LEDGER_LATENCY_MS} (default 150).
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String address = env("TEMPORAL_ADDRESS", "localhost:7233");
        String namespace = env("TEMPORAL_NAMESPACE", "default");
        String apiKey = env("TEMPORAL_API_KEY", "");
        int port = Integer.parseInt(env("HERITAGE_JAVA_PORT", "9103"));
        String ledgerUrl = env("HERITAGE_JAVA_URL", "http://localhost:" + port);
        long latencyMs = Long.parseLong(env("LEDGER_LATENCY_MS", "150"));

        Ledger ledger = new Ledger(latencyMs);
        LedgerHttpServer httpServer;
        try {
            httpServer = new LedgerHttpServer(ledger, port);
        } catch (java.net.BindException e) {
            log.error("Port {} is already in use; stop the other process or set HERITAGE_JAVA_PORT", port);
            System.exit(1);
            return;
        }
        httpServer.start();
        log.info("Heritage ledger HTTP API listening on http://localhost:{} (latency ~{} ms per call)", port, latencyMs);

        WorkflowServiceStubsOptions.Builder stubOptions = WorkflowServiceStubsOptions.newBuilder().setTarget(address);
        boolean cloud = !apiKey.isEmpty();
        if (cloud) {
            // TLS plus the API key. The SDK (1.28.1 and later) adds the temporal-namespace header itself;
            // adding it again here makes Temporal Cloud reject every call as unauthorized.
            stubOptions.setEnableHttps(true).addApiKey(() -> apiKey);
        }
        WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(stubOptions.build());

        CountDownLatch stopped = new CountDownLatch(1);
        AtomicReference<WorkerFactory> factoryRef = new AtomicReference<>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down");
            WorkerFactory factory = factoryRef.get();
            if (factory != null) {
                factory.shutdown();
                factory.awaitTermination(10, TimeUnit.SECONDS);
            }
            service.shutdown();
            httpServer.stop();
            stopped.countDown();
            log.info("Stopped");
        }, "shutdown"));

        // Wait for Temporal so the ledger API stays up even if the server starts later.
        while (true) {
            try {
                service.connect(Duration.ofSeconds(5));
                break;
            } catch (RuntimeException e) {
                if (stopped.getCount() == 0) {
                    return;
                }
                log.warn("Waiting for Temporal at {} (namespace={}): {}", address, namespace, e.getMessage());
                Thread.sleep(2000);
            }
        }

        // Set the identity explicitly: the SDK default resolves the local host name, which can stall for
        // several seconds on some laptops.
        String identity = ProcessHandle.current().pid() + "@heritage-java";
        WorkflowClient client = WorkflowClient.newInstance(
                service, WorkflowClientOptions.newBuilder().setNamespace(namespace).setIdentity(identity).build());
        WorkerFactory factory = WorkerFactory.newInstance(client);
        Worker worker = factory.newWorker(LedgerActivities.TASK_QUEUE);
        worker.registerActivitiesImplementations(new LedgerActivitiesImpl(ledgerUrl));
        factoryRef.set(factory);
        factory.start();
        log.info("Java ledger Worker polling '{}' on {} (namespace={}){}",
                LedgerActivities.TASK_QUEUE, address, namespace, cloud ? " with TLS and API key auth" : "");

        stopped.await();
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
