package com.lodh.myproduct.myapp;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

final class GreetingResource {
    private static String mongoFindOne() {
        ConnectionString connectionString = new ConnectionString("mongodb://u:p@localhost");
        try (MongoClient client = MongoClients.create(new MyCustomizer().customize(
                MongoClientSettings.builder().applyConnectionString(connectionString)).build())) {
            return String.valueOf(client.getDatabase("CRMGATEWAY_LODH_SANDBOX")
                    .getCollection("portfolios")
                    .find()
                    .first());
        }
    }

    public static void main(String[] args) {
        System.out.println(mongoFindOne());
    }
}
