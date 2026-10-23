package heritage.ledger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The ledger HTTP API on top of {@link Ledger}, using the JDK's built-in HTTP server. */
public final class LedgerHttpServer {

    private static final Logger log = LoggerFactory.getLogger(LedgerHttpServer.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() { };

    private final Ledger ledger;
    private final HttpServer server;
    private final ExecutorService executor;

    public LedgerHttpServer(Ledger ledger, int port) throws IOException {
        this.ledger = ledger;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(executor);
        server.createContext("/", this::handle);
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
        executor.shutdownNow();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getPath();
            if (path.length() > 1 && path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
            Ledger.Result result;
            try {
                result = route(method, path, exchange);
            } catch (MethodNotAllowed e) {
                result = error(405, e.getMessage());
            } catch (BadRequest e) {
                result = error(400, e.getMessage());
            } catch (RuntimeException e) {
                log.warn("{} {} failed: {}", method, path, e.toString());
                result = error(500, "internal error");
            }
            send(exchange, result);
        }
    }

    private Ledger.Result route(String method, String path, HttpExchange exchange) throws IOException {
        switch (path) {
            case "/ledger/hold":
                requireMethod(method, "POST");
                return ledger.hold(body(exchange));
            case "/ledger/commit":
                requireMethod(method, "POST");
                return ledger.commit(body(exchange));
            case "/ledger/release":
                requireMethod(method, "POST");
                return ledger.release(body(exchange));
            case "/ledger/holds":
                requireMethod(method, "GET");
                return new Ledger.Result(200, ledger.holds());
            case "/admin/stats":
                requireMethod(method, "GET");
                return new Ledger.Result(200, ledger.stats());
            case "/admin/faults":
                if (method.equals("GET")) {
                    Map<String, Object> view = new LinkedHashMap<>();
                    view.put("ok", true);
                    view.put("faults", ledger.faults());
                    return new Ledger.Result(200, view);
                }
                requireMethod(method, "POST");
                return ledger.setFault(body(exchange));
            case "/admin/reset":
                requireMethod(method, "POST");
                ledger.reset();
                return new Ledger.Result(200, Map.of("ok", true));
            default:
                return error(404, "not found: " + path);
        }
    }

    private static void requireMethod(String actual, String expected) {
        if (!actual.equals(expected)) {
            throw new MethodNotAllowed(expected);
        }
    }

    private static Map<String, Object> body(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            byte[] bytes = in.readAllBytes();
            if (bytes.length == 0) {
                return Map.of();
            }
            try {
                Map<String, Object> parsed = JSON.readValue(bytes, MAP);
                return parsed == null ? Map.of() : parsed;
            } catch (IOException e) {
                throw new BadRequest("request body must be a JSON object");
            }
        }
    }

    private static void send(HttpExchange exchange, Ledger.Result result) throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(result.body());
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(result.status(), bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static Ledger.Result error(int status, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", message);
        return new Ledger.Result(status, body);
    }

    private static class BadRequest extends RuntimeException {
        BadRequest(String message) {
            super(message);
        }
    }

    private static final class MethodNotAllowed extends BadRequest {
        MethodNotAllowed(String expected) {
            super("method not allowed; use " + expected);
        }
    }
}
