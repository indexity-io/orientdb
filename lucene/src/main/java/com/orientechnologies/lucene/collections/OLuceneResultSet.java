/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.lucene.collections;

import com.codahale.metrics.Timer;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.stream.OStream;
import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import com.orientechnologies.lucene.engine.OLuceneIndexEngineAbstract;
import com.orientechnologies.lucene.engine.OLuceneIndexEngineUtils;
import com.orientechnologies.lucene.exception.OLuceneIndexException;
import com.orientechnologies.lucene.functions.OLuceneFunctionsUtils;
import com.orientechnologies.lucene.query.OLuceneQueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChangesAbstract;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryTermScorer;
import org.apache.lucene.search.highlight.Scorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.highlight.TokenSources;

/** Created by Enrico Risa on 16/09/15. */
public class OLuceneResultSet {

  private final Query query;
  private final String reportQueryAs;
  private final OLuceneIndexEngine engine;
  private final OLuceneQueryContext queryContext;
  private final String indexName;
  private final long deletedMatchCount;
  private final Highlighter highlighter;
  private final List<String> highlighted;
  private final int maxNumFragments;
  private final String distinctKey;

  public OLuceneResultSet(
      final OLuceneIndexEngine engine,
      final OLuceneQueryContext queryContext,
      final ODocument metadata) {
    this.engine = engine;
    this.queryContext = queryContext;
    this.query = queryContext.getQuery();
    if (OLuceneFunctionsUtils.getReportQueryAs(metadata) != null) {
      this.reportQueryAs = OLuceneFunctionsUtils.getReportQueryAs(metadata);
    } else {
      this.reportQueryAs = query.toString();
    }
    this.indexName = engine.indexName();
    this.deletedMatchCount = calculateDeletedMatch();

    final Map<String, Object> highlight =
        Optional.ofNullable(metadata.<Map>getProperty("highlight")).orElse(Collections.emptyMap());

    highlighted =
        Optional.ofNullable((List<String>) highlight.get("fields")).orElse(Collections.emptyList());

    final String startElement = (String) Optional.ofNullable(highlight.get("start")).orElse("<B>");

    final String endElement = (String) Optional.ofNullable(highlight.get("end")).orElse("</B>");

    final Scorer scorer = new QueryTermScorer(queryContext.getQuery());
    final Formatter formatter = new SimpleHTMLFormatter(startElement, endElement);
    highlighter = new Highlighter(formatter, scorer);

    maxNumFragments = (int) Optional.ofNullable(highlight.get("maxNumFragments")).orElse(2);

    final String distinctBy = metadata.getProperty("distinctBy");
    if ("@rid".equals(distinctBy)) {
      this.distinctKey = OLuceneIndexEngineAbstract.RID;
    } else {
      this.distinctKey = distinctBy;
    }
  }

  private long calculateDeletedMatch() {
    return queryContext.deletedDocs(query);
  }

  public OStream<OIdentifiable> stream() {
    final OLuceneResultSetSpliteratorTx results = new OLuceneResultSetSpliteratorTx();
    return OStream.stream(results).onClose(results::close);
  }

  private class OLuceneResultSetSpliteratorTx implements Spliterator<OIdentifiable> {

    private final long maxReturnedHits;
    private final HashSet<String> distinctIds;

    private ScoreDoc[] scoreDocs;
    private int index;
    private int localIndex;
    private volatile boolean closed = false;

    public OLuceneResultSetSpliteratorTx() {
      final Long queryMaxHits = OLuceneFunctionsUtils.getResultLimit(queryContext.getContext());
      long maxHits = (queryMaxHits == null) ? Long.MAX_VALUE : queryMaxHits;

      final TopDocs topDocs = fetchMoreResult(null, maxHits);
      long totalHits = topDocs.totalHits - deletedMatchCount;

      final long softLimit = OGlobalConfiguration.LUCENE_RESULTS_SOFT_LIMIT.getValueAsLong();
      final long hardLimit = OGlobalConfiguration.LUCENE_RESULTS_HARD_LIMIT.getValueAsLong();
      long resultHits = Math.max(0, Math.min(maxHits, totalHits));
      if (resultHits > softLimit) {
        engine.recordSoftLimitExceeded();
        OLogManager.instance()
            .warn(
                this,
                "Results returned by Lucene query '%s' exceeds soft limit (%d vs %d)",
                reportQueryAs,
                resultHits,
                softLimit);
      } else {
        if (resultHits > hardLimit) {
          engine.recordHardLimitExceeded();
          OLogManager.instance()
              .warn(
                  this,
                  "Results returned by Lucene query '%s' exceeds hard limit (%d vs %d)",
                  reportQueryAs,
                  resultHits,
                  hardLimit);
          resultHits = hardLimit;
        }
      }
      this.maxReturnedHits = resultHits;
      engine.recordHits(totalHits, maxReturnedHits);
      OLuceneIndexEngineUtils.sendTotalHits(
          indexName, queryContext.getContext(), totalHits, maxReturnedHits);

      distinctIds = (distinctKey == null) ? null : new HashSet<>((int) Math.min(1000, resultHits));
    }

    public void close() {
      if (!closed) {
        final IndexSearcher searcher = queryContext.getSearcher();
        engine.release(searcher);
        closed = true;
        final long fetchedHits = index + scoreDocs.length - localIndex;
        engine.recordFetchedHits(fetchedHits, index);
      }
    }

    @Override
    public boolean tryAdvance(Consumer<? super OIdentifiable> action) {
      if (closed) {
        throw new IllegalStateException("ResultSet is closed");
      }
      final boolean hasNext = (index < maxReturnedHits);
      if (!hasNext) {
        return false;
      }

      OContextualRecordId res;
      Document doc;
      do {
        ScoreDoc scoreDoc = fetchNext();
        if (scoreDoc == null) {
          return false;
        }
        doc = toDocument(scoreDoc);

        res = toRecordId(doc, scoreDoc);
      } while (isToSkip(res, doc));
      index++;
      action.accept(res);
      return true;
    }

    @Override
    public Spliterator<OIdentifiable> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return (distinctIds == null) ? maxReturnedHits : Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
      return ORDERED | SIZED | NONNULL;
    }

    protected ScoreDoc fetchNext() {
      if (scoreDocs.length == 0) {
        return null;
      }
      if (localIndex == scoreDocs.length) {
        localIndex = 0;
        fetchMoreResult(scoreDocs[scoreDocs.length - 1], maxReturnedHits - index);
        if (scoreDocs.length == 0) {
          return null;
        }
      }
      final ScoreDoc score = scoreDocs[localIndex++];
      return score;
    }

    private Document toDocument(final ScoreDoc score) {
      try {
        return queryContext.getSearcher().doc(score.doc);
      } catch (final IOException e) {
        OLogManager.instance().error(this, "Error during conversion to document", e);
        return null;
      }
    }

    private OContextualRecordId toRecordId(final Document doc, final ScoreDoc score) {
      final String rId = doc.get(OLuceneIndexEngineAbstract.RID);
      final OContextualRecordId res = new OContextualRecordId(rId);

      final IndexReader indexReader = queryContext.getSearcher().getIndexReader();
      try {
        for (final String field : highlighted) {
          final String text = doc.get(field);
          if (text != null) {
            TokenStream tokenStream =
                TokenSources.getAnyTokenStream(
                    indexReader, score.doc, field, doc, engine.indexAnalyzer());
            TextFragment[] frag =
                highlighter.getBestTextFragments(tokenStream, text, true, maxNumFragments);
            queryContext.addHighlightFragment(field, frag);
          }
        }
        engine.onRecordAddedToResultSet(queryContext, res, doc, score);
        return res;
      } catch (IOException | InvalidTokenOffsetsException e) {
        throw OException.wrapException(new OLuceneIndexException("error while highlighting"), e);
      }
    }

    private boolean isToSkip(final OContextualRecordId recordId, final Document doc) {
      return isDeleted(recordId, doc) || isUpdatedDiskMatch(recordId, doc) || !isDistinct(doc);
    }

    private TopDocs fetchMoreResult(ScoreDoc after, long maxHits) {
      try (final Timer.Context fetch = engine.fetch()) {
        final TopDocs topDocs;
        final IndexSearcher searcher = queryContext.getSearcher();
        int pageSize =
            (int)
                Math.min(
                    ((distinctIds == null) ? 1 : 3) * maxHits,
                    OGlobalConfiguration.LUCENE_RESULTS_PAGE_SIZE.getValueAsInteger());
        if (queryContext.getSort() == null) {
          topDocs = searcher.searchAfter(after, query, pageSize);
        } else {
          topDocs = searcher.searchAfter(after, query, pageSize, queryContext.getSort());
        }
        scoreDocs = topDocs.scoreDocs;
        return topDocs;
      } catch (final IOException e) {
        OLogManager.instance()
            .error(
                this, "Error on fetching document by query '%s' to Lucene index", e, reportQueryAs);
        throw new OLuceneIndexException(
            String.format(
                "Error on fetching document by query '%s' to Lucene index", reportQueryAs));
      }
    }

    private boolean isDeleted(OIdentifiable value, Document doc) {
      return queryContext.isDeleted(doc, null, value);
    }

    private boolean isUpdatedDiskMatch(OIdentifiable value, Document doc) {
      return isUpdated(value) && !isTempMatch(doc);
    }

    private boolean isUpdated(OIdentifiable value) {
      return queryContext.isUpdated(null, null, value);
    }

    private boolean isTempMatch(Document doc) {
      return doc.get(OLuceneTxChangesAbstract.TMP) != null;
    }

    private boolean isDistinct(Document doc) {
      if (distinctIds == null) {
        return true;
      }
      final String distinctId = doc.get(distinctKey);
      return (distinctId == null) || distinctIds.add(distinctId);
    }
  }
}
