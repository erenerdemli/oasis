package io.github.oasis.services.events;

import io.github.oasis.services.events.utils.TestUtils;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Isuru Weerarathna
 */
@DisplayName("Payload Format")
public class PayloadFormatTest extends AbstractEventPushTest {

    @Test
    @DisplayName("Server: Payload content type incorrect")
    void payloadContentTypeIncorrect(Vertx vertx, VertxTestContext testContext) throws NoSuchAlgorithmException {
        KeyPair keyPair = TestUtils.createKeys();
        vertx.deployVerticle(createKnownSource(keyPair), testContext.succeeding());

        String payload = "isuru";
        String hash = TestUtils.signPayload(payload, keyPair.getPrivate());

        callForEvent(vertx, KNOWN_SOURCE + ":" + hash)
                .sendBuffer(
                        Buffer.buffer(payload),
                        testContext.succeeding(res -> assert400Response(res, testContext))
                );
    }

    @Test
    @DisplayName("Payload does not have a data field")
    void payloadFormatIncorrect(Vertx vertx, VertxTestContext testContext) throws NoSuchAlgorithmException {
        KeyPair keyPair = TestUtils.createKeys();
        vertx.deployVerticle(createKnownSource(keyPair), testContext.succeeding());

        JsonObject payload = new JsonObject().put("name", "isuru");
        String hash = TestUtils.signPayload(payload, keyPair.getPrivate());

        callForEvent(vertx, KNOWN_SOURCE + ":" + hash)
                .sendJson(
                        payload,
                        testContext.succeeding(res -> assert400Response(res, testContext))
                );
    }

    @Test
    @DisplayName("No such user exists")
    void authSuccess(Vertx vertx, VertxTestContext testContext) throws NoSuchAlgorithmException {
        KeyPair keyPair = TestUtils.createKeys();
        vertx.deployVerticle(createKnownSource(keyPair), testContext.succeeding());

        JsonObject event = TestUtils.aEvent("unknown@oasis.com", System.currentTimeMillis(), "test.a", 100);
        JsonObject payload = new JsonObject().put("data", event);
        String hash = TestUtils.signPayload(payload, keyPair.getPrivate());

        callForEvent(vertx, KNOWN_SOURCE + ":" + hash)
                .sendJson(
                        payload,
                        testContext.succeeding(res -> {
                            testContext.verify(() -> {
                                assertThat(res.statusCode()).isEqualTo(400);
                                verifyPushTimes(0);
                                testContext.completeNow();
                            });
                        }));
    }

    private void assert400Response(HttpResponse<String> response, VertxTestContext ctx) {
        ctx.verify(() -> {
            assertThat(response.statusCode()).isEqualTo(400);
            ctx.completeNow();
        });
    }

}
