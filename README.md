Run `mvn verify` with an implementation of Java SE 23, and you'll see

```
[ERROR] Failed to execute goal org.apache.maven.plugins:maven-compiler-plugin:3.13.0:compile (default-compile) on project api: Compilation failure
[ERROR] /Users/valentin.kovalenko/Documents/programming/projects/unexpected-generics-behavior/src/main/java/internal/Concrete.java:[5,1] api.Base is not public in api; cannot be accessed from outside package
```

If you make `Base` public, then `mvn verify` succeeds.
And you can use [`javap`](https://docs.oracle.com/en/java/javase/23/docs/specs/man/javap.html)
to see what is inside `Concrete.class`:

```shell
$ javap -v ./target/classes/internal/Concrete.class
...
public api.Interface method();
...
public api.Base method();
...
```

The `api.Base method()` is what causes the compilation error.
I do not know why the type chosen is `api.Base`, and not `java.lang.Object`.
