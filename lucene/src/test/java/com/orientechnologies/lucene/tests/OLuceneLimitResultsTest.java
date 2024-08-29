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
  public void testLimitSelect() {
    OResultSet docs =
        db.query(
            "select *,$totalHits,$Song_title_totalHits,$returnedHits,$Song_title_returnedHits "
                + "from Song where search_class('title:man', {\"limit\":\"select\"})= true limit 1");

    List<OResult> results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(1);

    OResult doc = results.get(0);
    System.out.println("doc.toElement().toJSON() = " + doc.toElement().toJSON());

    assertThat(doc.<Long>getProperty("$totalHits")).isEqualTo(14L);
    assertThat(doc.<Long>getProperty("$Song_title_totalHits")).isEqualTo(14L);
    assertThat(doc.<Long>getProperty("$returnedHits")).isEqualTo(1L);
    assertThat(doc.<Long>getProperty("$Song_title_returnedHits")).isEqualTo(1L);
    docs.close();

    docs =
        db.query(
            "select *,$totalHits,$Song_title_totalHits,$returnedHits,$Song_title_returnedHits "
                + "from Song where search_class('title:man', {\"limit\":\"select\"})= true skip 5 limit 5");

    results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(5);

    doc = results.get(0);
    System.out.println("doc.toElement().toJSON() = " + doc.toElement().toJSON());

    assertThat(doc.<Long>getProperty("$totalHits")).isEqualTo(14L);
    assertThat(doc.<Long>getProperty("$Song_title_totalHits")).isEqualTo(14L);
    assertThat(doc.<Long>getProperty("$returnedHits")).isEqualTo(10L);
    assertThat(doc.<Long>getProperty("$Song_title_returnedHits")).isEqualTo(10L);
    docs.close();
  }

  @Test
  public void testLimitByNumber() {
    OResultSet docs =
        db.query(
            "select *,$totalHits,$Song_title_totalHits,$returnedHits,$Song_title_returnedHits from Song "
                + "where search_class('title:man', {\"limit\": 5})= true limit 1");

    List<OResult> results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(1);

    OResult doc = results.get(0);
    System.out.println("doc.toElement().toJSON() = " + doc.toElement().toJSON());

    assertThat(doc.<Long>getProperty("$totalHits")).isEqualTo(14L);
    assertThat(doc.<Long>getProperty("$Song_title_totalHits")).isEqualTo(14L);
    assertThat(doc.<Long>getProperty("$returnedHits")).isEqualTo(5L);
    assertThat(doc.<Long>getProperty("$Song_title_returnedHits")).isEqualTo(5L);
    docs.close();

    docs =
        db.query(
            "select *,$totalHits,$Song_title_totalHits,$returnedHits,$Song_title_returnedHits from Song "
                + "where search_class('title:man', {\"limit\": 5})= true limit 10");

    results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(5);

    doc = results.get(0);
    System.out.println("doc.toElement().toJSON() = " + doc.toElement().toJSON());

    assertThat(doc.<Long>getProperty("$totalHits")).isEqualTo(14L);
    assertThat(doc.<Long>getProperty("$Song_title_totalHits")).isEqualTo(14L);
    assertThat(doc.<Long>getProperty("$returnedHits")).isEqualTo(5L);
    assertThat(doc.<Long>getProperty("$Song_title_returnedHits")).isEqualTo(5L);
    docs.close();
  }
}
