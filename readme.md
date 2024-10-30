### Prepare

#### Install GraalVM

[GraalVM for JDK 21 Community](https://github.com/graalvm/graalvm-ce-builds/releases/tag/jdk-21.0.2) is required.
Note that it is [available](https://sdkman.io/jdks#graalce) via [SDKMAN!](https://sdkman.io/).

#### Configure environment variables pointing to JDKs.

Set `GRAALVM_HOME`.
Assuming you installed version 21.0.2 of GraalVM for JDK 21 Community via SDKMAN!, run

```bash
export GRAALVM_HOME=$(realpath ~/".sdkman/candidates/java/21.0.2-graalce/")
```

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

Update `connectionString` in `./app/src/main/java/com/lodh/myproduct/myapp/GreetingResource.java`
with your connections string.

### Build and run

Run from the project root directory:

| &#x23; | Command                                | Description                             |
|--------|----------------------------------------|-----------------------------------------|
| 0      | `./gradlew clean :app:nativeCompile`   | Build an executable.                    |
| 1      | `./app/build/native/nativeCompile/app` | Run the executable that has been built. |
