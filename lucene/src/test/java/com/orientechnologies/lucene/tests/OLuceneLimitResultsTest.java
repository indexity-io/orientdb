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

package com.orientechnologies.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.lucene.exception.OLuceneIndexException;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class OLuceneLimitResultsTest extends OLuceneBaseTest {

  @Before
  public void init() {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream));

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void testLimitByNumber() {
    OResultSet docs =
        db.query(
            "select *,$totalHits,$Song_title_totalHits,$returnedHits,$Song_title_returnedHits from Song "
                + "where search_class('title:man', {\"limit\": 5})= true limit 1");

    checkLimitedResult(docs, 1, 5);

    docs =
        db.query(
            "select *,$totalHits,$Song_title_totalHits,$returnedHits,$Song_title_returnedHits from Song "
                + "where search_class('title:man', {\"limit\": 5})= true limit 10");

    checkLimitedResult(docs, 5, 5);
  }

  @Test
  public void testLimitSelect() {
    OResultSet docs =
        db.query(
            "select *,$totalHits,$Song_title_totalHits,$returnedHits,$Song_title_returnedHits "
                + "from Song where search_class('title:man', {\"limit\":\"select\"})= true limit 1");

    checkLimitedResult(docs, 1, 1);
  }

  @Test(expected = OLuceneIndexException.class)
  public void testLimitSelectRequiresLimit() {
    db.query("select * from Song where search_class('title:man', {\"limit\":\"select\"})= true");
  }

  @Test(expected = OLuceneIndexException.class)
  public void testLimitSelectDoesntUseParentLimit() {
    db.query(
        "select * from ("
            + "select * from Song where search_class('title:man', {\"limit\":\"select\"})= true)"
            + "limit 1");
  }

  @Test
  public void testLimitSelectNested() {
    OResultSet docs =
        db.query(
            "select * from ("
                + "select *,$totalHits,$Song_title_totalHits,$returnedHits,$Song_title_returnedHits from Song"
                + "   where search_class('title:man', {\"limit\":\"select\", \"inheritLimit\":true})= true)"
                + "limit 3");

    checkLimitedResult(docs, 3, 6);

    docs =
        db.query(
            "select * from ("
                + "select *,$totalHits,$Song_title_totalHits,$returnedHits,$Song_title_returnedHits from Song"
                + "   where search_class('title:man', {\"limit\":\"select\", \"inheritLimit\":true, \"inheritLimitMultiplier\":3})= true)"
                + "limit 3");

    checkLimitedResult(docs, 3, 9);
  }

  @Test
  public void testLimitWithInheritedAtTopLevel() {
    OResultSet docs =
        db.query(
            "select *,$totalHits,$Song_title_totalHits,$returnedHits,$Song_title_returnedHits from Song "
                + "where search_class('title:man', {\"limit\":\"select\", \"inheritLimit\":true})= true limit 1");

    checkLimitedResult(docs, 1, 1);
  }

  private void checkLimitedResult(OResultSet docs, int resultCount, long returnedHits) {
    List<OResult> results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(resultCount);

    OResult doc = results.get(0);

    assertThat(doc.<Long>getProperty("$totalHits")).isEqualTo(14L);
    assertThat(doc.<Long>getProperty("$Song_title_totalHits")).isEqualTo(14L);
    assertThat(doc.<Long>getProperty("$returnedHits")).isEqualTo(returnedHits);
    assertThat(doc.<Long>getProperty("$Song_title_returnedHits")).isEqualTo(returnedHits);
    docs.close();
  }
}
