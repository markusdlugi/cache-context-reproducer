package org.acme;

import static io.smallrye.mutiny.vertx.MutinyHelper.executor;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestQuery;

import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

@Path("/hello")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class GreetingResource {

    private static final Logger LOGGER = Logger.getLogger(GreetingResource.class);

    @Inject
    PersonRepository personRepository;

    @Inject
    SqsAsyncClient sqsAsyncClient;

    @CacheName("my-cache")
    Cache cache;

    @Inject
    Vertx vertx;

    @POST
    @WithTransaction
    public Uni<Person> hello(
            @RestQuery
            final String name) {
        return Uni.createFrom().item(new Person(name)) //
                .onItem().call(this::persistPerson) //
                .onItem().call(this::sendSqsMessage) //
                .onItem().call(this::activatePerson);
    }

    public Uni<Void> sendSqsMessage(final Person name) {
        return this.getQueueUrl("my-queue") //
                .onItem().transformToUni(queueUrl -> this.sendMessage(queueUrl, name.getName())) //
                .replaceWithVoid();
    }

    public Uni<String> getQueueUrl(final String queueName) {
        final Context context = vertx.getOrCreateContext();
        return cache.getAsync(queueName,
                key -> Uni.createFrom().completionStage(sqsAsyncClient.getQueueUrl(b -> b.queueName(queueName))) //
                        .emitOn(executor(context)) //
                        .onItem().transform(GetQueueUrlResponse::queueUrl))
                // FIXME: Adding this line to restore the context after cache invocation will make the test pass
                // .emitOn(executor(context))
                ;
    }

    public Uni<SendMessageResponse> sendMessage(final String queueUrl, final String message) {
        final Context context = vertx.getOrCreateContext();
        return Uni.createFrom()
                .completionStage(sqsAsyncClient.sendMessage(b -> b.queueUrl(queueUrl).messageBody(message))) //
                .emitOn(executor(context)) //
                .onItem().invoke(() -> LOGGER.info(String.format("Sent message '%s' to queue %s", message, queueUrl)));
    }

    public Uni<Person> persistPerson(final Person person) {
        return this.personRepository.persistAndFlush(person) //
                .onItem().invoke(() -> LOGGER.info("Persisted Person " + person.getName()));
    }

    public Uni<Void> activatePerson(final Person person) {
        return this.personRepository.findById(person.getId()) //
                .onItem().ifNotNull().invoke(p -> {
                    p.setStatus(Person.Status.ACTIVE);
                    LOGGER.info("Set status of Person " + p.getName() + " to active");
                }) //
                .replaceWithVoid();
    }
}
