package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OIndexableSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import java.util.Map;
import java.util.stream.Stream;

/** Created by frank on 25/05/2017. */
public abstract class OLuceneSearchFunctionTemplate extends OSQLFunctionAbstract
    implements OIndexableSQLFunction {

  public OLuceneSearchFunctionTemplate(String iName, int iMinParams, int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  @Override
  public boolean canExecuteInline(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {
    return allowsIndexedExecution(target, operator, rightValue, ctx, args);
  }

  @Override
  public boolean allowsIndexedExecution(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {
    OLuceneFullTextIndex index = searchForIndex(target, ctx, args);
    return index != null;
  }

  @Override
  public boolean shouldExecuteAfterSearch(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {
    return false;
  }

  @Override
  public long estimate(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {
    try (Stream<OIdentifiable> result = searchFromTarget(target, operator, rightValue, ctx, args)) {
      return (result == null) ? -1 : result.count();
    }
  }

  protected ODocument getMetadata(OExpression metadata, OCommandContext ctx) {
    final Object md = metadata.execute((OIdentifiable) null, ctx);
    if (md instanceof ODocument) {
      return (ODocument) md;
    } else if (md instanceof Map) {
      return new ODocument().fromMap((Map<String, ?>) md);
    } else if (md instanceof String) {
      return new ODocument().fromJSON((String) md);
    } else {
      return new ODocument().fromJSON(metadata.toString());
    }
  }

  protected abstract OLuceneFullTextIndex searchForIndex(
      OFromClause target, OCommandContext ctx, OExpression... args);
}
