# JSON Event Sourcing Test

This package provides a way to test the reducers in [JSON Event Sourcing](https://github.com/json-event-sourcing/pincette-jes.git). You give it a test directory in which you create the subdirectories ```commands``` and ```replies```. It sends the commands in alphabetical order to the command Kafka topic, which corresponds with the aggregate type of the command. It collects all the replies from the corresponding reply Kafka topics and compares the results with the files in the ```replies``` subdirectory. The differences are reported. If there is at least one difference the exit code will be 1 and 0 otherwise.

When comparing the actual with the expected results some technical fields are left out. These are ```_corr```, ```_jwt```, ```_seq``` and ```_timestamp```. If a command doesn't contain a ```_jwt``` field, one will be added with the username "system". Commands will always get a new ```_corr``` field.

You should run the tool in the same working directory as your application. It will load its configuration in the same way. At least the entry ```kafka``` is expected. The entry ```environment``` is optional. When it is not present the value "dev" will be used.

You can build the tool with ```mvn clean package``` and run it as follows:

```
> java -jar target/pincette-jes-test-<version>-jar-with-dependencies.jar -d test
```

When creating your test flows makes sure they are repeatable. Because the identifiers are reused it is best to start with the ```put``` command and end with the ```delete``` command for every aggregate instance in the test set.

There is also a small [API](https://www.javadoc.io/doc/net.pincette/pincette-jes-test/latest/index.html).