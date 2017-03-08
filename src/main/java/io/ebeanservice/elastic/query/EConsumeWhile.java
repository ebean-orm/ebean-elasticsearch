package io.ebeanservice.elastic.query;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * Used to process scroll while query results to the consumer.
 */
public interface EConsumeWhile<T> {

  /**
   * Process initial scroll query results.
   */
  boolean consumeInitialWhile(Predicate<T> consumer) throws IOException;

  /**
   * Process the next scroll results.
   */
  boolean consumeMoreWhile(Predicate<T> consumer) throws IOException;

  /**
   * Clear all scroll ids used.
   */
  void clearScrollIds();
}
