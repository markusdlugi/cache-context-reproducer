package org.acme;

import static io.restassured.RestAssured.given;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;

@QuarkusTest
@TestHTTPEndpoint(GreetingResource.class)
class ReproducerTest {

    @Test
    void callHelloEndpointInParallel() {
        final var names = List.of("Alice", "Bob", "Charlie");

        names.parallelStream().forEach(name -> //
                given().contentType(ContentType.JSON).when().queryParam("name", name).post().then().statusCode(200));
    }

}