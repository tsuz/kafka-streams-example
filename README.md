# kafka-streams-example
Kafka streams example


# Install

Ensure Maven is installed.

```
mvn install
```


# Run

1. Add a `client.properties` file in `src/main/resources/client.properties`.

2. Run either using:

(Replace KStreamGroupByCount with the file you'd like to run).

local
```
mvn exec:java -D exec.mainClass=com.mycompany.app.KStreamGroupByCount
```

Uber jar
```
mvn clean compile
java -cp  target/kstream-bootcamp-1.0-SNAPSHOT-jar-with-dependencies.jar com.mycompany.app.KStreamGroupByCount
```


