package com.avaje.ebeanservice.elastic.support;

import com.avaje.ebeanservice.elastic.search.BasicFieldsListener;
import com.avaje.ebeanservice.elastic.search.SearchResultParser;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class SearchResultParserTest {


  @Test
  public void test_example_1() throws IOException {


    InputStream is = SearchResultParser.class.getResourceAsStream("/search-results/example-1.json");


    JsonFactory jsonFactory = new JsonFactory();

    JsonParser jp = jsonFactory.createParser(is);

    BasicFieldsListener rows = new BasicFieldsListener();
    SearchResultParser parser = new SearchResultParser(jp, null);
    parser.read();

    System.out.print(rows.getRows());

  }
}