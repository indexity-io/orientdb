package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.resultset.OFilterResultSet;
import com.orientechnologies.orient.core.sql.executor.resultset.OLimitedResultSet;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.Optional;

/** Created by luigidellaquila on 01/03/17. */
public class FilterByClassStep extends AbstractExecutionStep {

  private OIdentifier identifier;
  private OResultSet prevResult = null;
  private long cost;

  public FilterByClassStep(OIdentifier identifier, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.identifier = identifier;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }

    return new OLimitedResultSet(
        new OFilterResultSet(() -> fetchNext(ctx, nRecords), this::filterMap), nRecords);
  }

  private OResult filterMap(OResult result) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      Optional<OElement> element = result.getElement();
      if (element.isPresent()) {
        Optional<OClass> clazz = element.get().getSchemaType();
        if (clazz.isPresent() && clazz.get().isSubClassOf(identifier.getStringValue())) {
          return result;
        }
      }
      return null;
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  private OResultSet fetchNext(OCommandContext ctx, int nRecords) {
    OExecutionStepInternal prevStep = prev.get();
    if (prevResult == null) {
      prevResult = prevStep.syncPull(ctx, nRecords);
    } else if (!prevResult.hasNext()) {
      prevResult = prevStep.syncPull(ctx, nRecords);
    }
    return prevResult;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder result = new StringBuilder();
    result.append(OExecutionStepInternal.getIndent(depth, indent));
    result.append("+ FILTER ITEMS BY CLASS");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    result.append(" \n");
    result.append(OExecutionStepInternal.getIndent(depth, indent));
    result.append("  ");
    result.append(identifier.getStringValue());
    return result.toString();
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    result.setProperty("identifier", identifier.serialize());

    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      identifier = OIdentifier.deserialize(fromResult.getProperty("identifier"));
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new FilterByClassStep(this.identifier.copy(), ctx, this.profilingEnabled);
  }
}
