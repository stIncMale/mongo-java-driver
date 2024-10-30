### Prepare

#### Install "Docker"

Install https://formulae.brew.sh/formula/docker, which looks like a set of CLI tools that are partially useless on their own.

#### Install "container runtime", or whatever this is

Install https://github.com/abiosoft/colima, though [OrbStack](https://orbstack.dev) will probably also work.
Start it by running `colima start --memory 5`.

#### Install a JDK

Install a JDK for Java SE 21.

#### Prepare the MongoDB deployment

Your deployment does not have to store any data, but if you want to,
you may insert a document by connecting to the deployment with [`mongosh`](https://www.mongodb.com/docs/mongodb-shell/)
and executing the following commands:

```js
use CRMGATEWAY_LODH_SANDBOX;
db.portfolios.insertOne({
  _id: 0,
  shortName: "abc"
});
```

You do not have to connect to your deployment with explicit credentials, but if you want to,
you may create a user `u` with password `p` by connecting to the deployment with [`mongosh`](https://www.mongodb.com/docs/mongodb-shell/)
and executing the following commands:

```js
use admin;
db.createUser({
    user: "u",
    pwd: "p",
    roles: ["root"]
});
```

#### Specify the MongoDB connection string

Update `connectionString` in `./src/main/java/com/lodh/myproduct/myapp/GreetingResource.java`
and `connection-string` in `./src/main/resources/application.yml`
with your connections string.

### Build and run as a native image

To build the application, run the following from the project root directory

```bash
mvn clean package -Dquarkus.native.resources.includes=application-local.yml -Dquarkus.native.additional-build-args="--strict-image-heap","-H:+UnlockExperimentalVMOptions","--diagnostics-mode","-H:Log=registerResource:5" -Pnative
docker build -f src/main/docker/Dockerfile.native-micro -t myapp .
```

To start the built application, run

```bash
docker run --name myapp -it --rm -e QUARKUS_PROFILE=local -p 18080:8080 -p 9000:9000 myapp
```

**To reproduce the issue**, run the following in a separate shell instance

```bash
curl localhost:18080/hello/mongo/first
```

You will see the following **error** in the application logs, which are outputted to the terminal

```
2024-10-30 17:03:29,055 ERROR [io.qua.ver.htt.run.QuarkusErrorHandler] (executor-thread-1) HTTP Request to /hello/mongo/first failed, error id: bfa175cb-fc4f-4166-83b0-07efb4712c8a-1: java.lang.UnsatisfiedLinkError: com.mongodb.internal.crypt.capi.CAPI.mongocrypt_new()Lcom/mongodb/internal/crypt/capi/CAPI$mongocrypt_t; [symbol: Java_com_mongodb_internal_crypt_capi_CAPI_mongocrypt_1new or Java_com_mongodb_internal_crypt_capi_CAPI_mongocrypt_1new__]
	at org.graalvm.nativeimage.builder/com.oracle.svm.core.jni.access.JNINativeLinkage.getOrFindEntryPoint(JNINativeLinkage.java:152)
	at org.graalvm.nativeimage.builder/com.oracle.svm.core.jni.JNIGeneratedMethodSupport.nativeCallAddress(JNIGeneratedMethodSupport.java:54)
	at com.mongodb.internal.crypt.capi.CAPI.mongocrypt_new(Native Method)
	at com.mongodb.internal.crypt.capi.MongoCryptImpl.<init>(MongoCryptImpl.java:116)
	at com.mongodb.internal.crypt.capi.MongoCrypts.create(MongoCrypts.java:40)
	at com.mongodb.client.internal.Crypts.createCrypt(Crypts.java:50)
	at com.mongodb.client.internal.MongoClientImpl.<init>(MongoClientImpl.java:95)
	at com.mongodb.client.internal.MongoClientImpl.<init>(MongoClientImpl.java:78)
	at com.mongodb.client.MongoClients.create(MongoClients.java:108)
	at com.mongodb.client.MongoClients.create(MongoClients.java:50)
	at com.lodh.myproduct.myapp.GreetingResource.mongoFindOne(GreetingResource.java:20)
	at com.lodh.myproduct.myapp.GreetingResource$quarkusrestinvoker$mongoFindOne_b65ea6c5a6941119de6883d945f1dfb598fc94a4.invoke(Unknown Source)
	at org.jboss.resteasy.reactive.server.handlers.InvocationHandler.handle(InvocationHandler.java:29)
	at io.quarkus.resteasy.reactive.server.runtime.QuarkusResteasyReactiveRequestContext.invokeHandler(QuarkusResteasyReactiveRequestContext.java:141)
	at org.jboss.resteasy.reactive.common.core.AbstractResteasyReactiveContext.run(AbstractResteasyReactiveContext.java:147)
	at io.quarkus.vertx.core.runtime.VertxCoreRecorder$14.runWith(VertxCoreRecorder.java:635)
	at org.jboss.threads.EnhancedQueueExecutor$Task.doRunWith(EnhancedQueueExecutor.java:2516)
	at org.jboss.threads.EnhancedQueueExecutor$Task.run(EnhancedQueueExecutor.java:2495)
	at org.jboss.threads.EnhancedQueueExecutor$ThreadBody.run(EnhancedQueueExecutor.java:1521)
	at org.jboss.threads.DelegatingRunnable.run(DelegatingRunnable.java:11)
	at org.jboss.threads.ThreadLocalResettingRunnable.run(ThreadLocalResettingRunnable.java:11)
	at io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
	at java.base@21.0.5/java.lang.Thread.runWith(Thread.java:1596)
	at java.base@21.0.5/java.lang.Thread.run(Thread.java:1583)
	at org.graalvm.nativeimage.builder/com.oracle.svm.core.thread.PlatformThreads.threadStartRoutine(PlatformThreads.java:896)
	at org.graalvm.nativeimage.builder/com.oracle.svm.core.thread.PlatformThreads.threadStartRoutine(PlatformThreads.java:872)
```

Note that on my machine the built application cannot connect from a container to my local MongoDB deployment,
but that is not relevant, as the issue still reproduces.

### Build and run as a normal Java application

Run either

```shell scrip
mvn clean package
docker build -f src/main/docker/Dockerfile.jvm -t myapp-jvm .
docker run --name myapp -it --rm -e QUARKUS_PROFILE=local -p 18080:18080 -p 9000:9000 myapp-jvm
```

 or

```shell script
mvn clean package
java -Dquarkus.profile=local -jar target/quarkus-app/quarkus-run.jar
```

Note that the issue does not reproduce this way.
