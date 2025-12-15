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
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class OLuceneDistinctTest extends OLuceneBaseTest {

  @Before
  public void init() {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneDistinctIndex.sql");

    db.execute("sql", getScriptFromStream(stream));

    db.command(
        "create index Entity.repeatedValue on Entity (distinctKey,repeatedValue) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void testNonDistinctRepeats() {
    OResultSet docs = db.query("select * from Entity where search_class('repeatedValue:a1')= true");

    List<OResult> results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(9);
    assertThat(new HashSet<>(results)).hasSize(results.size());

    docs = db.query("select id from Entity where search_class('repeatedValue:a2')= true");
    results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(6);
    assertThat(new HashSet<>(results)).hasSize(results.size());

    docs = db.query("select id from Entity where search_class('repeatedValue:a3')= true");
    results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(3);
    assertThat(new HashSet<>(results)).hasSize(results.size());
  }

  @Test
  public void testNonDistinctRepeatsWithWildcard() {
    OResultSet docs =
        db.query("select id from Entity where search_class('repeatedValue:a?')= true");

    List<OResult> results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(21);
    assertThat(new HashSet<>(results)).hasSize(12);

    docs = db.query("select id from Entity where search_class('repeatedValue:a2~1')= true");
    results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(21);
    assertThat(new HashSet<>(results)).hasSize(12);
  }

  @Test
  public void testDistinctByRid() {
    OResultSet docs =
        db.query(
            "select id from Entity where search_class('repeatedValue:a1', {\"distinctBy\":\"@rid\"})= true");

    List<OResult> results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(9);
    assertThat(new HashSet<>(results)).hasSize(results.size());

    docs =
        db.query(
            "select id from Entity where search_class('repeatedValue:a2', {\"distinctBy\":\"@rid\"})= true");
    results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(6);
    assertThat(new HashSet<>(results)).hasSize(results.size());

    docs =
        db.query(
            "select id from Entity where search_class('repeatedValue:a3', {\"distinctBy\":\"@rid\"})= true");
    results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(3);
    assertThat(new HashSet<>(results)).hasSize(results.size());
  }

  @Test
  public void testDistinctByRidWithWildcard() {
    OResultSet docs =
        db.query(
            "select id from Entity where search_class('repeatedValue:a?', {\"distinctBy\":\"@rid\"})= true");

    List<OResult> results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(12);
    assertThat(new HashSet<>(results)).hasSize(results.size());

    docs =
        db.query(
            "select id from Entity where search_class('repeatedValue:a1~1', {\"distinctBy\":\"@rid\"})= true");
    results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(12);
    assertThat(new HashSet<>(results)).hasSize(results.size());
  }

  @Test
  public void testCustomDistinctWithWildcard() {
    OResultSet docs =
        db.query(
            "select id from Entity where search_class('repeatedValue:a?', {\"distinctBy\":\"distinctKey\"})= true");

    List<OResult> results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(10);
    assertThat(new HashSet<>(results)).hasSize(7);

    docs =
        db.query(
            "select id from Entity where search_class('repeatedValue:a1~1', {\"distinctBy\":\"distinctKey\"})= true");
    results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(10);
    assertThat(new HashSet<>(results)).hasSize(7);
  }
}
