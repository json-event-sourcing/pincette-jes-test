package net.pincette.jes.test;

import javax.json.JsonArray;
import javax.json.JsonObject;

@FunctionalInterface
public interface Report {
  void report(
      final String filename,
      final JsonObject command,
      final JsonObject expected,
      final JsonArray patch);
}
