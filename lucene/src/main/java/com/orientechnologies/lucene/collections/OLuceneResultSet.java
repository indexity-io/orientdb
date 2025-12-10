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
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import java.util.Collections;
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

  // TODO: Make page size a global config item
  private static Integer PAGE_SIZE = 1000;
  private Query query;
  private OLuceneIndexEngine engine;
  private OLuceneQueryContext queryContext;
  private String indexName;
  private Highlighter highlighter;
  private List<String> highlighted;
  private int maxNumFragments;
  private TopDocs topDocs;
  private long deletedMatchCount = 0;
  private long returnedHits = 0;

  private volatile boolean closed = false;

  public OLuceneResultSet(
      final OLuceneIndexEngine engine,
      final OLuceneQueryContext queryContext,
      final ODocument metadata) {
    this.engine = engine;
    this.queryContext = queryContext;
    this.query = queryContext.getQuery();
    this.indexName = engine.indexName();

    fetchFirstBatch();
    deletedMatchCount = calculateDeletedMatch();

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

    final Long queryMaxHits = OLuceneFunctionsUtils.getResultLimit(queryContext.getContext());
    long maxHits = (queryMaxHits == null) ? Integer.MAX_VALUE : queryMaxHits;
    // TODO: Implement a soft/hard cap on totalHits that can be iterated
    this.returnedHits = Math.max(0, Math.min(maxHits, topDocs.totalHits - deletedMatchCount));
  }

  protected void fetchFirstBatch() {
    try {
      final IndexSearcher searcher = queryContext.getSearcher();
      if (queryContext.getSort() == null) {
        topDocs = searcher.search(query, PAGE_SIZE);
      } else {
        topDocs = searcher.search(query, PAGE_SIZE, queryContext.getSort());
      }
    } catch (final IOException e) {
      OLogManager.instance()
          .error(this, "Error on fetching document by query '%s' to Lucene index", e, query);
    }
  }

  public void sendLookupTime(OCommandContext commandContext, long start) {
    OLuceneIndexEngineUtils.sendLookupTime(indexName, commandContext, topDocs, -1, start);
  }

  protected long calculateDeletedMatch() {
    return queryContext.deletedDocs(query);
  }

  private void close() {
    if (!closed) {
      final IndexSearcher searcher = queryContext.getSearcher();
      engine.release(searcher);
      closed = true;
    }
  }

  public OStream<OIdentifiable> stream() {
    return OStream.stream(new OLuceneResultSetSpliteratorTx()).onClose(this::close);
  }

  private class OLuceneResultSetSpliteratorTx implements Spliterator<OIdentifiable> {

    private ScoreDoc[] scoreDocs;
    private int index;
    private int localIndex;
    private long totalHits;

    public OLuceneResultSetSpliteratorTx() {
      totalHits = topDocs.totalHits;
      index = 0;
      localIndex = 0;
      scoreDocs = topDocs.scoreDocs;
      OLuceneIndexEngineUtils.sendTotalHits(
          indexName,
          queryContext.getContext(),
          topDocs.totalHits - deletedMatchCount,
          returnedHits);
    }

    @Override
    public boolean tryAdvance(Consumer<? super OIdentifiable> action) {
      if (closed) {
        throw new IllegalStateException("ResultSet is closed");
      }
      final boolean hasNext = (index < returnedHits);
      if (!hasNext) {
        return false;
      }

      OContextualRecordId res;
      Document doc;
      do {
        ScoreDoc scoreDoc = fetchNext();
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
      return returnedHits;
    }

    @Override
    public int characteristics() {
      return ORDERED | SIZED | NONNULL;
    }

    protected ScoreDoc fetchNext() {
      if (localIndex == scoreDocs.length) {
        localIndex = 0;
        fetchMoreResult();
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
      return isDeleted(recordId, doc) || isUpdatedDiskMatch(recordId, doc);
    }

    private void fetchMoreResult() {
      TopDocs topDocs = null;
      try {
        final IndexSearcher searcher = queryContext.getSearcher();
        final int pageSize = (int) Math.min(returnedHits - index, PAGE_SIZE);
        if (queryContext.getSort() == null) {
          topDocs = searcher.searchAfter(scoreDocs[scoreDocs.length - 1], query, pageSize);
        } else {
          topDocs =
              searcher.searchAfter(
                  scoreDocs[scoreDocs.length - 1], query, pageSize, queryContext.getSort());
        }
        scoreDocs = topDocs.scoreDocs;
      } catch (final IOException e) {
        OLogManager.instance()
            .error(this, "Error on fetching document by query '%s' to Lucene index", e, query);
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
  }
}
