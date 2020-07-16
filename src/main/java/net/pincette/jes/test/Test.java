package net.pincette.jes.test;

import static java.nio.file.Files.list;
import static java.util.Arrays.asList;
import static java.util.Comparator.comparing;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getGlobal;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.JWT;
import static net.pincette.jes.util.JsonFields.SEQ;
import static net.pincette.jes.util.JsonFields.SUB;
import static net.pincette.jes.util.JsonFields.TIMESTAMP;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.jes.util.Streams.streamsConfig;
import static net.pincette.json.JsonUtil.copy;
import static net.pincette.json.JsonUtil.createDiff;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.composeAsyncStream;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.zip;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetWithRethrow;
import static org.apache.kafka.streams.KafkaStreams.State.ERROR;
import static org.apache.kafka.streams.KafkaStreams.State.PENDING_SHUTDOWN;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonObject;
import net.pincette.function.SideEffect;
import net.pincette.jes.util.JsonSerializer;
import net.pincette.util.Pair;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * This class can run a set of reducer tests. In the test directory it takes the JSON files in
 * alphabetical order from the "commands" subdirectory and publishes them on the command Kafka
 * topic, which corresponds to their aggregate type (_type field). It then collects all the replies
 * from the reply Kafka topics and compares them with the expected results in the "replies"
 * subdirectory.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Test {
  private static final String APPLICATION_ID = "application.id";
  private static final Set<String> TECHNICAL = set(CORR, JWT, SEQ, TIMESTAMP);

  private Test() {}

  private static void addResult(
      final List<JsonObject> results,
      final List<Pair<String, JsonObject>> commands,
      final JsonObject reply) {
    Optional.ofNullable(reply.getString(CORR, null))
        .flatMap(corr -> getIndex(commands, corr))
        .ifPresent(index -> results.set(index, reply));
  }

  private static Map<String, Object> asMap(final Properties properties) {
    return properties.entrySet().stream().collect(toMap(e -> (String) e.getKey(), Entry::getValue));
  }

  private static void adjustApplicationId(final Properties kafkaConfig) {
    Optional.ofNullable(kafkaConfig.getProperty(APPLICATION_ID))
        .filter(id -> !id.endsWith("-test"))
        .ifPresent(id -> kafkaConfig.setProperty(APPLICATION_ID, id + "-test"));
  }

  private static void collectReplies(
      final StreamsBuilder builder,
      final String type,
      final String environment,
      final Map<String, JsonObject> perCorr,
      final Consumer<JsonObject> reply) {
    final KStream<String, JsonObject> stream = builder.stream(type + "-reply-" + environment);

    stream
        .filter(
            (k, v) ->
                Optional.ofNullable(v.getString(CORR, null))
                    .filter(perCorr::containsKey)
                    .isPresent())
        .mapValues(v -> SideEffect.<JsonObject>run(() -> reply.accept(v)).andThenGet(() -> v));
  }

  private static Stream<JsonObject> commandStream(final List<Pair<String, JsonObject>> commands) {
    return commands.stream().map(pair -> pair.second);
  }

  private static boolean compareResults(
      final List<JsonObject> results,
      final List<JsonObject> replies,
      final List<Pair<String, JsonObject>> commands,
      final Report report) {
    return rangeExclusive(0, results.size())
        .map(
            i ->
                pair(
                    i,
                    createDiff(removeTechnical(replies.get(i)), removeTechnical(results.get(i)))
                        .toJsonArray()))
        .map(
            pair ->
                SideEffect.<JsonArray>run(
                        () ->
                            report.report(
                                commands.get(pair.first).first,
                                commands.get(pair.first).second,
                                replies.get(pair.first),
                                pair.second))
                    .andThenGet(() -> pair.second))
        .reduce(true, (result, diff) -> result && diff.isEmpty(), (r1, r2) -> r1);
  }

  private static boolean complete(final List<JsonObject> results) {
    return results.stream().noneMatch(Objects::isNull);
  }

  private static JsonObject completeCommand(final JsonObject json) {
    return create(() -> createObjectBuilder(json))
        .update(b -> b.add(CORR, randomUUID().toString()))
        .updateIf(
            b -> !json.containsKey(JWT), b -> b.add(JWT, createObjectBuilder().add(SUB, "system")))
        .build()
        .build();
  }

  private static Optional<Integer> getIndex(
      final List<Pair<String, JsonObject>> commands, final String corr) {
    return zip(commands.stream(), rangeExclusive(0, commands.size()))
        .filter(pair -> pair.first.second.getString(CORR).equals(corr))
        .findFirst()
        .map(pair -> pair.second);
  }

  private static List<Pair<String, JsonObject>> loadCommands(final Path directory) {
    return loadJson(directory.resolve("commands"))
        .map(pair -> pair(pair.first, completeCommand(pair.second)))
        .collect(toList());
  }

  private static Stream<Pair<String, JsonObject>> loadJson(final Path directory) {
    return tryToGetRethrow(() -> list(directory))
        .orElseGet(Stream::empty)
        .filter(path -> path.toFile().isFile())
        .filter(Files::isReadable)
        .sorted(comparing(Path::getFileName))
        .map(path -> pair(path.getFileName().toString(), parse(path)));
  }

  private static List<JsonObject> loadReplies(final Path directory) {
    return loadJson(directory.resolve("replies")).map(pair -> pair.second).collect(toList());
  }

  private static JsonObject parse(final Path path) {
    return tryToGetRethrow(() -> createReader(new FileInputStream(path.toFile())).readObject())
        .orElse(null);
  }

  private static Map<String, JsonObject> perCorrelationId(final Stream<JsonObject> commands) {
    return commands.collect(toMap(c -> c.getString(CORR), c -> c));
  }

  /**
   * Runs a test set in a directory.
   *
   * @param directory the test directory. It should have the subdirectories "commands" and
   *     "replies", which should have an equal number of JSON files in them. The files will be
   *     ordered by their filename.
   * @param kafkaConfig the Kafka configuration.
   * @param environment the environment tag, which is appended to the command and reply topics.
   * @param report the reporter, which is called for each test. This is where you can create the
   *     structure of your application.
   * @return The outcome of all the tests. If at least one test fails the result will be <code>false
   *     </code>.
   * @since 1.0
   */
  public static boolean run(
      final Path directory,
      final Properties kafkaConfig,
      final String environment,
      final Report report) {
    return run(directory, kafkaConfig, environment, report, null);
  }

  /**
   * Runs a test set in a directory.
   *
   * @param directory the test directory. It should have the subdirectories "commands" and
   *     "replies", which should have an equal number of JSON files in them. The files will be
   *     ordered by their filename.
   * @param kafkaConfig the Kafka configuration.
   * @param environment the environment tag, which is appended to the command and reply topics.
   * @param report the reporter, which is called for each test.
   * @param buildBefore if this is not <code>null</code> it is called before running the tests. This
   *     is where you can create the structure of your application.
   * @return The outcome of all the tests. If at least one test fails the result will be <code>false
   *     </code>.
   * @since 1.0.1
   */
  public static boolean run(
      final Path directory,
      final Properties kafkaConfig,
      final String environment,
      final Report report,
      final UnaryOperator<StreamsBuilder> buildBefore) {
    final List<Pair<String, JsonObject>> commands = loadCommands(directory);
    final List<JsonObject> replies = loadReplies(directory);

    if (commands.size() != replies.size()) {
      getGlobal().log(SEVERE, "There should be as many replies as there are commands.");

      return false;
    }

    adjustApplicationId(kafkaConfig);

    final StreamsBuilder builder = new StreamsBuilder();
    final boolean[] error = new boolean[1];
    final CountDownLatch latch = new CountDownLatch(1);
    final Map<String, JsonObject> perCorr = perCorrelationId(commandStream(commands));
    final List<JsonObject> results = new ArrayList<>(asList(new JsonObject[replies.size()]));
    final KafkaStreams[] streams = new KafkaStreams[1];

    if (buildBefore != null) {
      buildBefore.apply(builder);
    }

    types(commandStream(commands))
        .forEach(
            type ->
                collectReplies(
                    builder,
                    type,
                    environment,
                    perCorr,
                    reply -> {
                      addResult(results, commands, reply);
                      Optional.ofNullable(streams[0])
                          .filter(s -> complete(results))
                          .ifPresent(KafkaStreams::close);
                    }));

    streams[0] = new KafkaStreams(builder.build(), streamsConfig(kafkaConfig));
    streams[0].setStateListener(
        (newState, oldState) ->
            Optional.of(newState)
                .filter(state -> state == ERROR || state == PENDING_SHUTDOWN)
                .ifPresent(
                    state -> {
                      if (state == ERROR) {
                        error[0] = true;
                      }

                      latch.countDown();
                    }));

    streams[0].start();
    sendCommands(commandStream(commands), kafkaConfig, environment);
    tryToDoRethrow(latch::await);

    return !error[0] && compareResults(results, replies, commands, report);
  }

  private static JsonObject removeTechnical(final JsonObject json) {
    return copy(json, createObjectBuilder(), field -> !TECHNICAL.contains(field)).build();
  }

  private static CompletionStage<Boolean> sendCommands(
      final Stream<JsonObject> commands, final Properties kafkaConfig, final String environment) {
    return tryToGetWithRethrow(
            () ->
                createReliableProducer(
                    asMap(kafkaConfig), new StringSerializer(), new JsonSerializer()),
            producer ->
                composeAsyncStream(
                        commands.map(
                            command ->
                                send(
                                    producer,
                                    new ProducerRecord<>(
                                        command.getString(TYPE) + "-command-" + environment,
                                        command.getString(ID),
                                        command))))
                    .thenApply(results -> results.reduce((r1, r2) -> r1 && r2).orElse(true)))
        .orElseGet(() -> completedFuture(false));
  }

  private static Set<String> types(final Stream<JsonObject> commands) {
    return commands
        .map(json -> Optional.ofNullable(json.getString(TYPE, null)).orElse(null))
        .filter(Objects::nonNull)
        .collect(toSet());
  }
}
