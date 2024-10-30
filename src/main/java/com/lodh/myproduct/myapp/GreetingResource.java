package com.lodh.myproduct.myapp;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import com.mongodb.client.MongoClient;

@Path("/hello")
public class GreetingResource {
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/mongo/first")
    public String mongoFindOne() {
        ConnectionString connectionString = new ConnectionString("mongodb://u:p@localhost");
        try (MongoClient client = MongoClients.create(new MyCustomizer().customize(
                MongoClientSettings.builder().applyConnectionString(connectionString)).build())) {
            return String.valueOf(client.getDatabase("CRMGATEWAY_LODH_SANDBOX")
                    .getCollection("portfolios")
                    .find()
                    .first());
        }
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        return "Hello from Quarkus";
    }
}
