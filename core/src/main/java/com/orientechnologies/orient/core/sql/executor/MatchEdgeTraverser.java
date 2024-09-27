package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.ORid;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/** Created by luigidellaquila on 23/09/16. */
public class MatchEdgeTraverser {
  protected OResult sourceRecord;
  protected EdgeTraversal edge;
  protected OMatchPathItem item;
  protected Iterator<OResultInternal> downstream;

  public MatchEdgeTraverser(OResult lastUpstreamRecord, EdgeTraversal edge) {
    this.sourceRecord = lastUpstreamRecord;
    this.edge = edge;
    this.item = edge.edge.item;
  }

  public MatchEdgeTraverser(OResult lastUpstreamRecord, OMatchPathItem item) {
    this.sourceRecord = lastUpstreamRecord;
    this.item = item;
  }

  public boolean hasNext(OCommandContext ctx) {
    init(ctx);
    return downstream.hasNext();
  }

  public OResult next(OCommandContext ctx) {
    init(ctx);
    if (!downstream.hasNext()) {
      throw new IllegalStateException();
    }
    String endPointAlias = getEndpointAlias();
    OResultInternal nextR = downstream.next();
    OIdentifiable nextElement = nextR.getElement().get();
    Object prevValue = sourceRecord.getProperty(endPointAlias);
    if (prevValue != null && !equals(prevValue, nextElement)) {
      return null;
    }
    OResultInternal result = new OResultInternal();
    for (String prop : sourceRecord.getPropertyNames()) {
      result.setProperty(prop, sourceRecord.getProperty(prop));
    }
    result.setProperty(endPointAlias, toResult(nextElement));
    if (edge.edge.item.getFilter().getDepthAlias() != null) {
      result.setProperty(edge.edge.item.getFilter().getDepthAlias(), nextR.getMetadata("$depth"));
    }
    if (edge.edge.item.getFilter().getPathAlias() != null) {
      result.setProperty(
          edge.edge.item.getFilter().getPathAlias(), nextR.getMetadata("$matchPath"));
    }
    return result;
  }

  protected boolean equals(Object prevValue, OIdentifiable nextElement) {
    if (prevValue instanceof OResult) {
      prevValue = ((OResult) prevValue).getElement().orElse(null);
    }
    if (nextElement instanceof OResult) {
      nextElement = ((OResult) nextElement).getElement().orElse(null);
    }
    return prevValue != null && prevValue.equals(nextElement);
  }

  protected Object toResult(OIdentifiable nextElement) {
    return new OResultInternal(nextElement);
  }

  protected String getStartingPointAlias() {
    return this.edge.edge.out.alias;
  }

  protected String getEndpointAlias() {
    if (this.item != null) {
      return this.item.getFilter().getAlias();
    }
    return this.edge.edge.in.alias;
  }

  protected void init(OCommandContext ctx) {
    if (downstream == null) {
      Object startingElem = sourceRecord.getProperty(getStartingPointAlias());
      if (startingElem instanceof OResult) {
        startingElem = ((OResult) startingElem).getElement().orElse(null);
      }
      downstream =
          executeTraversal(ctx, this.item, (OIdentifiable) startingElem, 0, null).iterator();
    }
  }

  protected Iterable<OResultInternal> executeTraversal(
      OCommandContext iCommandContext,
      OMatchPathItem item,
      Object startingPoint,
      int depth,
      List<OIdentifiable> pathToHere) {
    OResult startResult;
    OIdentifiable startingPointId;
    if (startingPoint instanceof OResult) {
      startResult = (OResult) startingPoint;
      startingPointId = startResult.getElement().orElse(null);
    } else if (startingPoint != null) {
      startingPointId = (OIdentifiable) startingPoint;
      startResult = new OResultInternal(startingPointId);
    } else {
      startResult = null;
      startingPointId = null;
    }
    OWhereClause filter = null;
    OWhereClause whileCondition = null;
    Integer maxDepth = null;
    String className = null;
    Integer clusterId = null;
    ORid targetRid = null;
    if (item.getFilter() != null) {
      filter = getTargetFilter(item);
      whileCondition = item.getFilter().getWhileCondition();
      maxDepth = item.getFilter().getMaxDepth();
      className = targetClassName(item, iCommandContext);
      String clusterName = targetClusterName(item, iCommandContext);
      if (clusterName != null) {
        clusterId = iCommandContext.getDatabase().getClusterIdByName(clusterName);
      }
      targetRid = targetRid(item, iCommandContext);
    }

    Iterable<OResultInternal> result;

    if (whileCondition == null
        && maxDepth
            == null) { // in this case starting point is not returned and only one level depth is
      // evaluated

      Iterable<OResultInternal> queryResult = traversePatternEdge(startingPointId, iCommandContext);
      final OWhereClause theFilter = filter;
      final String theClassName = className;
      final Integer theClusterId = clusterId;
      final ORid theTargetRid = targetRid;
      result =
          () -> {
            Iterator<OResultInternal> iter = queryResult.iterator();

            return new Iterator() {

              private OResultInternal nextElement = null;

              @Override
              public boolean hasNext() {
                if (nextElement == null) {
                  fetchNext();
                }
                return nextElement != null;
              }

              @Override
              public Object next() {
                if (nextElement == null) {
                  fetchNext();
                }
                if (nextElement == null) {
                  throw new IllegalStateException();
                }
                OResultInternal res = nextElement;
                nextElement = null;
                return res;
              }

              public void fetchNext() {
                Object previousMatch = iCommandContext.getVariable("$currentMatch");
                OResultInternal matched = (OResultInternal) iCommandContext.getVariable("matched");
                if (matched != null) {
                  matched.setProperty(
                      getStartingPointAlias(), sourceRecord.getProperty(getStartingPointAlias()));
                }
                while (iter.hasNext()) {
                  OResultInternal next = iter.next();
                  iCommandContext.setVariable("$currentMatch", next);
                  if (matchesFilters(iCommandContext, theFilter, next)
                      && matchesClass(iCommandContext, theClassName, next)
                      && matchesCluster(iCommandContext, theClusterId, next)
                      && matchesRid(iCommandContext, theTargetRid, next)) {
                    nextElement = next;
                    break;
                  }
                }
                iCommandContext.setVariable("$currentMatch", previousMatch);
              }
            };
          };

    } else { // in this case also zero level (starting point) is considered and traversal depth is
      // given by the while condition
      result = new ArrayList<>();
      iCommandContext.setVariable("$depth", depth);
      Object previousMatch = iCommandContext.getVariable("$currentMatch");
      iCommandContext.setVariable("$currentMatch", startingPoint);

      if (matchesFilters(iCommandContext, filter, startResult)
          && matchesClass(iCommandContext, className, startResult)
          && matchesCluster(iCommandContext, clusterId, startResult)
          && matchesRid(iCommandContext, targetRid, startResult)) {
        OResultInternal rs = new OResultInternal(startingPointId);
        // set traversal depth in the metadata
        rs.setMetadata("$depth", depth);
        // set traversal path in the metadata
        rs.setMetadata("$matchPath", pathToHere == null ? Collections.EMPTY_LIST : pathToHere);
        // add the result to the list
        ((List) result).add(rs);
      }

      if ((maxDepth == null || depth < maxDepth)
          && (whileCondition == null
              || whileCondition.matchesFilters(startResult, iCommandContext))) {

        Iterable<OResultInternal> queryResult =
            traversePatternEdge(startingPointId, iCommandContext);

        for (OResultInternal origin : queryResult) {
          //          if(origin.equals(startingPoint)){
          //            continue;
          //          }
          // TODO consider break strategies (eg. re-traverse nodes)

          List<OIdentifiable> newPath = new ArrayList<>();
          if (pathToHere != null) {
            newPath.addAll(pathToHere);
          }

          OElement elem = origin.toElement();
          newPath.add(elem.getIdentity());

          Iterable<OResultInternal> subResult =
              executeTraversal(iCommandContext, item, elem, depth + 1, newPath);
          if (subResult instanceof Collection) {
            ((List) result).addAll((Collection<? extends OResultInternal>) subResult);
          } else {
            for (OResultInternal i : subResult) {
              ((List) result).add(i);
            }
          }
        }
      }
      iCommandContext.setVariable("$currentMatch", previousMatch);
    }
    return result;
  }

  protected OWhereClause getTargetFilter(OMatchPathItem item) {
    return item.getFilter().getFilter();
  }

  protected String targetClassName(OMatchPathItem item, OCommandContext iCommandContext) {
    return item.getFilter().getClassName(iCommandContext);
  }

  protected String targetClusterName(OMatchPathItem item, OCommandContext iCommandContext) {
    return item.getFilter().getClusterName(iCommandContext);
  }

  protected ORid targetRid(OMatchPathItem item, OCommandContext iCommandContext) {
    return item.getFilter().getRid(iCommandContext);
  }

  private boolean matchesClass(OCommandContext iCommandContext, String className, OResult origin) {
    if (className == null) {
      return true;
    }

    if (origin.getElement().isPresent()) {
      Optional<OClass> clazz = origin.getElement().get().getSchemaType();
      if (!clazz.isPresent()) {
        return false;
      }
      return clazz.get().isSubClassOf(className);
    }
    return false;
  }

  private boolean matchesCluster(
      OCommandContext iCommandContext, Integer clusterId, OResult origin) {
    if (clusterId == null) {
      return true;
    }
    if (origin == null) {
      return false;
    }

    if (origin.getIdentity().isEmpty()) {
      return false;
    }
    return clusterId.equals(origin.getIdentity().get().getClusterId());
  }

  private boolean matchesRid(OCommandContext iCommandContext, ORid rid, OResult origin) {
    if (rid == null) {
      return true;
    }
    if (origin == null) {
      return false;
    }

    if (origin.getIdentity().isEmpty()) {
      return false;
    }
    return origin.getIdentity().get().equals(rid.toRecordId(origin, iCommandContext));
  }

  protected boolean matchesFilters(
      OCommandContext iCommandContext, OWhereClause filter, OResult origin) {
    return filter == null || filter.matchesFilters(origin, iCommandContext);
  }

  // TODO refactor this method to receive the item.

  protected Iterable<OResultInternal> traversePatternEdge(
      OIdentifiable startingPoint, OCommandContext iCommandContext) {

    Iterable possibleResults = null;
    if (this.item.getFilter() != null) {
      String alias = getEndpointAlias();
      Object matchedNodes =
          iCommandContext.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
      if (matchedNodes != null) {
        if (matchedNodes instanceof Iterable) {
          possibleResults = (Iterable) matchedNodes;
        } else {
          possibleResults = Collections.singleton(matchedNodes);
        }
      }
    }

    Object prevCurrent = iCommandContext.getVariable("$current");
    iCommandContext.setVariable("$current", startingPoint);
    Object qR;
    try {
      qR = this.item.getMethod().execute(startingPoint, possibleResults, iCommandContext);
    } finally {
      iCommandContext.setVariable("$current", prevCurrent);
    }

    if (qR == null) {
      return Collections.EMPTY_LIST;
    }
    if (qR instanceof OIdentifiable) {
      return Collections.singleton(new OResultInternal((OIdentifiable) qR));
    }
    if (qR instanceof Iterable) {
      final Iterator<Object> iter = ((Iterable) qR).iterator();
      Iterable<OResultInternal> result =
          () ->
              new Iterator<OResultInternal>() {
                private OResultInternal nextElement;

                @Override
                public boolean hasNext() {
                  if (nextElement == null) {
                    fetchNext();
                  }
                  return nextElement != null;
                }

                @Override
                public OResultInternal next() {
                  if (nextElement == null) {
                    fetchNext();
                  }
                  if (nextElement == null) {
                    throw new IllegalStateException();
                  }
                  OResultInternal res = nextElement;
                  nextElement = null;
                  return res;
                }

                public void fetchNext() {
                  while (iter.hasNext()) {
                    Object o = iter.next();
                    if (o instanceof OIdentifiable) {
                      nextElement = new OResultInternal((OIdentifiable) o);
                      break;
                    } else if (o instanceof OResultInternal) {
                      nextElement = (OResultInternal) o;
                      break;
                    } else if (o == null) {
                      continue;
                    } else {
                      throw new UnsupportedOperationException();
                    }
                  }
                }
              };

      return result;
    }
    return Collections.EMPTY_LIST;
  }
}
