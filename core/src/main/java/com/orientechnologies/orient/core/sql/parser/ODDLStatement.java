package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.sql.executor.ODDLExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;

/** Created by luigidellaquila on 12/08/16. */
public abstract class ODDLStatement extends OStatement {

  public ODDLStatement(int id) {
    super(id);
  }

  public ODDLStatement(OrientSql p, int id) {
    super(p, id);
  }

  public abstract OResultSet executeDDL(OCommandContext ctx);

  public OResultSet doExecute(
      ODatabaseSession db, Map params, OCommandContext parentCtx, boolean usePlanCache) {
    OBasicCommandContext ctx = new OBasicCommandContext();
    if (parentCtx != null) {
      ctx.setParentWithoutOverridingChild(parentCtx);
    }
    ctx.setDatabase(db);
    ctx.setInputParameters(params);
    ODDLExecutionPlan executionPlan = (ODDLExecutionPlan) createExecutionPlan(ctx, false);
    return executionPlan.executeInternal(ctx);
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    return new ODDLExecutionPlan(ctx, this);
  }
}
