package io.github.oasis.services.events;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Isuru Weerarathna
 */
@DisplayName("Health Check")
public class PingTest extends AbstractTest {

    private static final String PING = "/ping";

    @Test
    @DisplayName("Server: ping")
    void healthCheck(Vertx vertx, VertxTestContext testContext) {
        callToEndPoint(PING, vertx)
                .as(BodyCodec.jsonObject())
                .send(testContext.succeeding(res -> {
                    testContext.verify(() -> {
                        JsonObject body = res.body();
                        assertTrue(body.containsKey("tz"));
                        assertTrue(body.containsKey("offset"));
                        assertTrue(body.containsKey("health"));
                        testContext.completeNow();
                    });
                }));
    }

}
