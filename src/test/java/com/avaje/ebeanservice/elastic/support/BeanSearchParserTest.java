package com.avaje.ebeanservice.elastic.support;

import com.avaje.ebeanservice.elastic.search.BasicFieldsListener;
import com.avaje.ebeanservice.elastic.search.BeanSearchParser;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class BeanSearchParserTest {


  @Test
  public void test_example_1() throws IOException {


    InputStream is = BeanSearchParser.class.getResourceAsStream("/search-results/example-1.json");


    JsonFactory jsonFactory = new JsonFactory();

    JsonParser jp = jsonFactory.createParser(is);

    BasicFieldsListener rows = new BasicFieldsListener();
    BeanSearchParser parser = new BeanSearchParser(jp, null, null, null);
    parser.read();

    System.out.print(rows.getRows());

  }
}