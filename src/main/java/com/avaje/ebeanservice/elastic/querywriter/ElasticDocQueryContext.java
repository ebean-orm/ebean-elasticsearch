package com.avaje.ebeanservice.elastic.querywriter;

import com.avaje.ebean.Expr;
import com.avaje.ebean.LikeType;
import com.avaje.ebean.OrderBy;
import com.avaje.ebean.PersistenceIOException;
import com.avaje.ebean.TextJunction;
import com.avaje.ebean.plugin.BeanType;
import com.avaje.ebean.plugin.ExpressionPath;
import com.avaje.ebean.search.Match;
import com.avaje.ebean.search.MultiMatch;
import com.avaje.ebean.search.TextCommonTerms;
import com.avaje.ebean.search.TextQueryString;
import com.avaje.ebean.search.TextSimple;
import com.avaje.ebeaninternal.api.SpiExpression;
import com.avaje.ebeaninternal.api.SpiExpressionList;
import com.avaje.ebeaninternal.api.SpiQuery;
import com.avaje.ebeaninternal.server.expression.DocQueryContext;
import com.avaje.ebeaninternal.server.expression.Op;
import com.avaje.ebeaninternal.server.query.SplitName;
import com.avaje.ebeaninternal.server.querydefn.OrmQueryDetail;
import com.avaje.ebeaninternal.server.querydefn.OrmQueryProperties;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Context for writing elastic search expressions.
 */
public class ElasticDocQueryContext implements DocQueryContext {

  private static final TextJunction.Type MUST = TextJunction.Type.MUST;
  private static final TextJunction.Type SHOULD = TextJunction.Type.SHOULD;
  private static final TextJunction.Type MUST_NOT = TextJunction.Type.MUST_NOT;

  private static final String BOOL = "bool";
  private static final String TERM = "term";
  private static final String RANGE = "range";
  private static final String TERMS = "terms";
  private static final String IDS = "ids";
  private static final String VALUES = "values";
  private static final String PREFIX = "prefix";
  private static final String WILDCARD = "wildcard";
  private static final String EXISTS = "exists";
  private static final String FIELD = "field";

  private final ElasticJsonContext context;

  private final SpiQuery<?> query;

  private final JsonGenerator json;

  private final StringWriter writer;

  private final BeanType<?> desc;

  private String currentNestedPath;

  /**
   * Return the query in ElasticSearch JSON form.
   */
  public static String asJson(ElasticJsonContext context, SpiQuery<?> query) {
    return new ElasticDocQueryContext(context, query).asElasticQuery();
  }

  /**
   * Construct given the JSON generator and root bean type.
   */
  private ElasticDocQueryContext(ElasticJsonContext context, SpiQuery<?> query) {
    this.context = context;
    this.query = query;
    this.desc = query.getBeanDescriptor();
    this.writer = new StringWriter(200);
    this.json = context.createGenerator(writer);

    desc.addInheritanceWhere(query);
  }

  private String asElasticQuery() {
    try {
      writeElastic(query);
      String jsonQuery = flush();

      query.setGeneratedSql(jsonQuery);
      return jsonQuery;

    } catch (IOException e) {
      throw new PersistenceIOException(e);
    }
  }

  private void writeElastic(SpiQuery<?> query) throws IOException {

    json.writeStartObject();
    writePaging(query);
    writeFetchPartial(query.getDetail());
    writeOrderBy(query.getOrderBy());

    json.writeFieldName("query");

    boolean hasFullText = writeFullText(query);
    writeFilter(query, hasFullText);

    json.writeEndObject();
  }

  private void writeFilter(SpiQuery<?> query, boolean hasFullText) throws IOException {

    SpiExpression idEquals = null;
    if (query.getId() != null) {
      idEquals = (SpiExpression) Expr.idEq(query.getId());
    }

    SpiExpressionList<?> where = query.getWhereExpressions();
    boolean hasWhere = (where != null && !where.isEmpty());
    if (idEquals != null || hasWhere) {
      if (!hasFullText) {
        json.writeStartObject();
        json.writeFieldName("filtered");
        json.writeStartObject();
      }
      json.writeFieldName("filter");
      if (hasWhere) {
        where.writeDocQuery(this, idEquals);
      } else {
        idEquals.writeDocQuery(this);
      }
      if (!hasFullText) {
        json.writeEndObject();
        json.writeEndObject();
      }
    } else if (!hasFullText) {
      writeMatchAll();
    }
  }

  private boolean writeFullText(SpiQuery<?> query) throws IOException {

    SpiExpressionList<?> text = query.getTextExpression();
    if (text != null && !text.isEmpty()) {
      text.writeDocQuery(this);
      return true;
    }
    return false;
  }

  private void writeMatchAll() throws IOException {
    json.writeStartObject();
    json.writeObjectFieldStart("match_all");
    json.writeEndObject();
    json.writeEndObject();
  }

  private void writePaging(SpiQuery<?> query) throws IOException {
    if (query.getFirstRow() > 0) {
      json.writeNumberField("from", query.getFirstRow());
    }
    if (query.getMaxRows() > 0) {
      json.writeNumberField("size", query.getMaxRows());
    }
  }

  /**
   * Write the Elastic search source include and fields if necessary for partial fetching.
   * <p>
   * Fetch all property is put into includes.
   * Fetch on 'many' path is put into includes.
   * Fetch on 'one' paths and root path are put into fields.
   * </p>
   */
  private void writeFetchPartial(OrmQueryDetail detail) throws IOException {

    Set<String> includes = new LinkedHashSet<String>();
    Set<String> fields = new LinkedHashSet<String>();

    for (Map.Entry<String, OrmQueryProperties> entry : detail.entries()) {

      String path = entry.getKey();
      OrmQueryProperties value = entry.getValue();
      if (value.allProperties()) {
        includes.add(path + ".*");
      } else if (containsMany(path)) {
        for (String propName : value.getIncluded()) {
          includes.add(path + "." + propName);
        }
      } else {
        for (String propName : value.getIncluded()) {
          fields.add(path + "." + propName);
        }
      }
    }

    OrmQueryProperties rootProps = detail.getChunk(null, false);
    if (rootProps.hasSelectClause()) {
      Set<String> included = rootProps.getIncluded();
      if (included != null) {
        for (String propName : included) {
          fields.add(propName);
        }
      }
    }

    if (!includes.isEmpty()) {
      json.writeFieldName("_source");
      json.writeStartObject();
      json.writeFieldName("include");
      json.writeStartArray();
      for (String propName : includes) {
        json.writeString(propName);
      }
      json.writeEndArray();
      json.writeEndObject();
    }

    if (!fields.isEmpty()) {
      json.writeFieldName("fields");
      json.writeStartArray();
      for (String propName : fields) {
        json.writeString(propName);
      }
      json.writeEndArray();
    }
  }

  /**
   * Flush the JsonGenerator buffer.
   */
  public String flush() throws IOException {
    endNested();
    json.flush();
    return writer.toString();
  }

  /**
   * Return true if the path contains a many.
   */
  private boolean containsMany(String path) {
    ExpressionPath elPath = desc.getExpressionPath(path);
    return elPath == null || elPath.containsMany();
  }

  /**
   * Return an associated 'raw' property given the property name.
   * This just returns the original propertyName if no 'raw' property is mapped.
   */
  private String rawProperty(String propertyName) {
    return desc.root().docStore().rawProperty(propertyName);
  }

  /**
   * Start Bool MUST or SHOULD.
   * <p>
   * If conjunction is true then MUST(and) and if false is SHOULD(or).
   */
  @Override
  public void startBool(boolean conjunction) throws IOException {
    writeBoolStart((conjunction) ? MUST : SHOULD);
  }

  /**
   * Start Bool MUST.
   */
  @Override
  public void startBoolMust() throws IOException {
    writeBoolStart(MUST);
  }

  /**
   * Start Bool MUST_NOT.
   */
  @Override
  public void startBoolMustNot() throws IOException {
    writeBoolStart(MUST_NOT);
  }

  /**
   * Start a Bool which could contain a MUST, SHOULD or MUST_NOT.
   */
  @Override
  public void startBoolGroup() throws IOException {
    json.writeStartObject();
    json.writeObjectFieldStart(BOOL);
  }

  @Override
  public void startBoolGroupList(TextJunction.Type type) throws IOException {
    json.writeArrayFieldStart(type.literal());
  }

  /**
   * Start a Bool expression list with the given type (MUST, MUST_NOT, SHOULD).
   */
  private void writeBoolStart(TextJunction.Type type) throws IOException {
    endNested();
    startBoolGroup();
    startBoolGroupList(type);
  }

  @Override
  public void endBoolGroupList() throws IOException {
    json.writeEndArray();
  }

  /**
   * Write the end of a Bool expression list.
   */
  @Override
  public void endBoolGroup() throws IOException {
    json.writeEndObject();
    json.writeEndObject();
  }

  /**
   * Write the end of a Bool expression list.
   */
  @Override
  public void endBool() throws IOException {
    endBoolGroupList();
    endBoolGroup();
  }

  @Override
  public void writeAllEquals(Map<String, Object> propMap) throws IOException {
    startBoolMust();
    for (Map.Entry<String, Object> entry : propMap.entrySet()) {
      Object value = entry.getValue();
      String propName = entry.getKey();
      if (value == null) {
        writeExists(false, propName);
      } else {
        writeEqualTo(propName, value);
      }
    }
    endBool();
  }

  @Override
  public void writeLike(String propName, String val, LikeType type, boolean caseInsensitive) throws IOException {
    switch (type) {
      case RAW:
        writeLike(propName, val);
        break;

      case STARTS_WITH:
        writeStartsWith(propName, val);
        break;

      case ENDS_WITH:
        writeEndsWith(propName, val);
        break;

      case CONTAINS:
        writeContains(propName, val);
        break;

      case EQUAL_TO:
        if (caseInsensitive) {
          writeIEqualTo(propName, val);
        } else {
          writeEqualTo(propName, val);
        }
        break;

      default:
        throw new RuntimeException("LikeType " + type + " missed?");
    }
  }

  /**
   * Write a term expression.
   */
  @Override
  public void writeEqualTo(String propertyName, Object value) throws IOException {

    // prepareNested on propertyName and expression uses raw
    prepareNestedPath(propertyName);
    writeRawExpression(TERM, rawProperty(propertyName), value);
  }

  /**
   * Write a range expression with a single value.
   */
  @Override
  public void writeRange(String propertyName, String rangeType, Object value) throws IOException {

    prepareNestedPath(propertyName);
    json.writeStartObject();
    json.writeObjectFieldStart(RANGE);
    json.writeObjectFieldStart(rawProperty(propertyName));
    json.writeFieldName(rangeType);
    context.writeScalar(json, value);
    json.writeEndObject();
    json.writeEndObject();
    json.writeEndObject();
  }

  /**
   * Write a range expression with a low and high value.
   */
  @Override
  public void writeRange(String propertyName, Op lowOp, Object valueLow, Op highOp, Object valueHigh) throws IOException {

    prepareNestedPath(propertyName);
    json.writeStartObject();
    json.writeObjectFieldStart(RANGE);
    json.writeObjectFieldStart(rawProperty(propertyName));
    json.writeFieldName(lowOp.docExp());
    context.writeScalar(json, valueLow);
    json.writeFieldName(highOp.docExp());
    context.writeScalar(json, valueHigh);
    json.writeEndObject();
    json.writeEndObject();
    json.writeEndObject();
  }

  /**
   * Write a terms expression.
   */
  @Override
  public void writeIn(String propertyName, Object[] values, boolean not) throws IOException {

    prepareNestedPath(propertyName);
    if (not) {
      startBoolMustNot();
    }
    json.writeStartObject();
    json.writeObjectFieldStart(TERMS);
    json.writeArrayFieldStart(rawProperty(propertyName));
    for (Object value : values) {
      context.writeScalar(json, value);
    }
    json.writeEndArray();
    json.writeEndObject();
    json.writeEndObject();
    if (not) {
      endBool();
    }
  }

  /**
   * Write an Ids expression.
   */
  @Override
  public void writeIds(List<?> idList) throws IOException {

    endNested();
    json.writeStartObject();
    json.writeObjectFieldStart(IDS);
    json.writeArrayFieldStart(VALUES);
    for (Object id : idList) {
      context.writeScalar(json, id);
    }
    json.writeEndArray();
    json.writeEndObject();
    json.writeEndObject();
  }

  /**
   * Write an Id expression.
   */
  @Override
  public void writeId(Object value) throws IOException {

    List<Object> ids = new ArrayList<Object>(1);
    ids.add(value);
    writeIds(ids);
  }

  /**
   * Write a prefix expression.
   */
  private void writeStartsWith(String propertyName, String value) throws IOException {
    // use analysed field
    writeRawWithPrepareNested(PREFIX, propertyName, value.toLowerCase());
  }

  /**
   * Suffix expression not supported yet.
   */
  private void writeEndsWith(String propertyName, String value) throws IOException {
    // use analysed field
    // this will likely be slow - best to avoid if you can
    writeWildcard(propertyName, "*" + value.toLowerCase());
  }

  /**
   * Write a match expression.
   */
  private void writeContains(String propertyName, String value) throws IOException {
    // use analysed field
    writeWildcard(propertyName, "*" + value.toLowerCase() + "*");
  }

  /**
   * Write a wildcard expression.
   */
  private void writeLike(String propertyName, String value) throws IOException {
    // use analysed field
    String val = value.toLowerCase();
    // replace SQL wildcard characters with ElasticSearch ones
    val = val.replace('_', '?');
    val = val.replace('%', '*');
    writeRawWithPrepareNested(WILDCARD, propertyName, val);
  }

  /**
   * Write case-insensitive equal to.
   */
  @Override
  public void writeIEqualTo(String propName, String value) throws IOException {

    String[] values = value.toLowerCase().split(" ");
    if (values.length == 1) {
      writeMatch(propName, value, null);
    } else {
      // Boolean AND all the terms together
      startBool(true);
      for (String val : values) {
        writeMatch(propName, val, null);
      }
      endBool();
    }
  }

  /**
   * Write a prefix expression.
   */
  public void writeMatch(String propertyName, String value, Match options) throws IOException {

    // use analysed field
    prepareNestedPath(propertyName);
    context.writeMatch(json, propertyName, value, options);
  }

  @Override
  public void writeMultiMatch(String search, MultiMatch options) throws IOException {

    // assuming fields are not in nested path
    context.writeMultiMatch(json, search, options);
  }

  @Override
  public void writeTextSimple(String search, TextSimple options) throws IOException {
    context.writeSimple(json, search, options);
  }

  @Override
  public void writeTextCommonTerms(String search, TextCommonTerms options) throws IOException {
    context.writeCommonTerms(json, search, options);
  }

  @Override
  public void writeTextQueryString(String search, TextQueryString options) throws IOException {
    context.writeQueryString(json, search, options);
  }

  /**
   * Write a wildcard expression.
   */
  private void writeWildcard(String propertyName, String value) throws IOException {
    writeRawWithPrepareNested(WILDCARD, propertyName, value);
  }

  /**
   * Write raw JSON to the query buffer.
   */
  @Override
  public void writeRaw(String raw, Object[] values) throws IOException {
    json.writeRaw(raw);
  }

  /**
   * Write an exists expression.
   */
  @Override
  public void writeExists(boolean notNull, String propertyName) throws IOException {

    // prepareNestedPath prior to BoolMustNotStart
    prepareNestedPath(propertyName);
    if (!notNull) {
      startBoolMustNot();
    }
    writeExists(propertyName);
    if (!notNull) {
      endBool();
    }
  }

  private void writeExists(String propertyName) throws IOException {
    writeRawExpression(EXISTS, FIELD, propertyName);
  }

  /**
   * Write with prepareNestedPath() on the propertyName
   */
  private void writeRawWithPrepareNested(String type, String propertyName, Object value) throws IOException {

    prepareNestedPath(propertyName);
    writeRawExpression(type, propertyName, value);
  }

  /**
   * Write raw.  prepareNestedPath() should already be done.
   */
  private void writeRawExpression(String type, String propertyName, Object value) throws IOException {

    json.writeStartObject();
    json.writeObjectFieldStart(type);
    json.writeFieldName(propertyName);
    context.writeScalar(json, value);
    json.writeEndObject();
    json.writeEndObject();
  }

  /**
   * Write an expression for the core operations.
   */
  @Override
  public void writeSimple(Op type, String propertyName, Object value) throws IOException {

    // prepareNested prior to boolMustNotStart
    prepareNestedPath(propertyName);
    switch (type) {
      case EQ:
        writeEqualTo(propertyName, value);
        break;
      case NOT_EQ:
        startBoolMustNot();
        writeEqualTo(propertyName, value);
        endBool();
        break;
      case EXISTS:
        writeExists(true, propertyName);
        break;
      case NOT_EXISTS:
        writeExists(false, propertyName);
        break;
      case BETWEEN:
        throw new IllegalStateException("BETWEEN Not expected in SimpleExpression?");

      default:
        writeRange(propertyName, type.docExp(), value);
    }
  }

  /**
   * Write the query sort.
   */
  public <T> void writeOrderBy(OrderBy<T> orderBy) throws IOException {

    if (orderBy != null && !orderBy.isEmpty()) {
      json.writeArrayFieldStart("sort");
      for (OrderBy.Property property : orderBy.getProperties()) {
        json.writeStartObject();
        json.writeObjectFieldStart(rawProperty(property.getProperty()));
        json.writeStringField("order", property.isAscending() ? "asc" : "desc");
        json.writeEndObject();
        json.writeEndObject();
      }
      json.writeEndArray();
    }
  }

  /**
   * Check if we need to start a nested path filter and do so if required.
   */
  private void prepareNestedPath(String propName) throws IOException {
    ExpressionPath exprPath = desc.getExpressionPath(propName);
    if (exprPath != null && exprPath.containsMany()) {
      String[] manyPath = SplitName.splitBegin(propName);
      startNested(manyPath[0]);
    } else {
      endNested();
    }
  }

  /**
   * Start a nested path filter.
   */
  private void startNested(String nestedPath) throws IOException {

    if (currentNestedPath != null) {
      if (currentNestedPath.equals(nestedPath)) {
        // just add to currentNestedPath
        return;
      } else {
        // end the prior one as this is different
        endNested();
      }
    }
    currentNestedPath = nestedPath;

    json.writeStartObject();
    json.writeObjectFieldStart("nested");
    json.writeStringField("path", nestedPath);
    json.writeFieldName("filter");
  }

  /**
   * End a nested path filter if one is still open.
   */
  private void endNested() throws IOException {
    if (currentNestedPath != null) {
      currentNestedPath = null;
      json.writeEndObject();
      json.writeEndObject();
    }
  }

}
