package net.pincette.jes.test;

import static java.lang.System.exit;
import static java.util.Arrays.stream;
import static net.pincette.jes.test.Test.run;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.jes.util.Streams.fromConfig;
import static net.pincette.util.Json.string;
import static net.pincette.util.Util.tryToGetSilent;

import com.typesafe.config.Config;
import java.io.File;
import java.util.Map;
import javax.json.JsonArray;
import javax.json.JsonObject;
import net.pincette.util.ArgsBuilder;

/**
 * The command-line for running reducer tests for JSON Event Sourcing. The configuration is loaded
 * from "conf/application.conf". It should have the "kafka" entry and the "environment" entry. The
 * latter will be set to "dev" when not present. The "-d" or "--directory" option should point to a
 * directory, which contains the subdirectories "commands" and "replies".
 *
 * @author Werner Donn\u00e9
 */
public class Application {
  private static final String DEV = "dev";
  private static final String DIRECTORY = "directory";
  private static final String DIRECTORY_OPT = "--directory";
  private static final String DIRECTORY_OPT_SHORT = "-d";
  private static final String ENVIRONMENT = "environment";
  private static final String KAFKA = "kafka";

  private static ArgsBuilder adder(final ArgsBuilder builder, final String arg) {
    switch (arg) {
      case DIRECTORY_OPT:
      case DIRECTORY_OPT_SHORT:
        return builder.addPending(DIRECTORY);
      default:
        return builder.add(arg);
    }
  }

  public static void main(final String[] args) {
    stream(args)
        .reduce(new ArgsBuilder(), Application::adder, (b1, b2) -> b1)
        .build()
        .filter(map -> map.containsKey(DIRECTORY))
        .map(Application::test)
        .orElse(Application::usage)
        .run();

    exit(0);
  }

  @SuppressWarnings("squid:S106") // Not logging.
  private static void report(
      final String filename,
      final JsonObject command,
      final JsonObject expected,
      final JsonArray patch) {
    System.out.println(filename);

    if (!patch.isEmpty()) {
      System.out.println("ERROR");
      System.out.println("The reply for the command");
      System.out.println(string(command));
      System.out.println("and the expected result");
      System.out.println(string(expected));
      System.out.println("have the following differences");
      System.out.println(string(patch));
    } else {
      System.out.println("OK");
    }
  }

  @SuppressWarnings("squid:S106") // Not logging.
  private static Runnable test(final Map<String, String> options) {
    return () -> {
      final Config config = loadDefault();

      if (!run(
          new File(options.get(DIRECTORY)).toPath(),
          fromConfig(config, KAFKA),
          tryToGetSilent(() -> config.getString(ENVIRONMENT)).orElse(DEV),
          Application::report)) {
        System.out.println("Some tests failed");
        exit(1);
      }

      System.out.println("Tests succeeded");
    };
  }

  @SuppressWarnings("squid:S106") // Not logging.
  private static void usage() {
    System.err.println("Usage: net.pincette.jes.test.Application (-d | --directory) directory");
    exit(1);
  }
}
