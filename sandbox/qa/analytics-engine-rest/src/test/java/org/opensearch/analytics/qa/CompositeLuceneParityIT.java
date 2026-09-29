/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

/**
 * A/B correctness: the same PPL query must return the same count against a composite
 * (parquet primary + lucene secondary) index and against a plain-Lucene index, at every
 * delete density.
 *
 * <p>The two indexes differ only in {@code index.pluggable.dataformat.enabled}. That single
 * setting decides both the engine ({@code DataFormatAwareEngine} vs {@code InternalEngine})
 * and, through the opensearch-sql plugin's {@code RestUnifiedQueryAction#isPluggableDataformatIndex},
 * whether PPL routes to the analytics engine. Queries go through {@code POST /_plugins/_ppl}
 * rather than the {@code /_analytics/ppl} shim precisely so that routing decision is exercised —
 * the shim would send both sides to the analytics engine and destroy the comparison.
 *
 * <p>What each side actually runs: {@code TransportPPLQueryAction} calls
 * {@code isAnalyticsIndex}, and on true hands the query to {@code RestUnifiedQueryAction} →
 * {@code AnalyticsExecutionEngine} (Calcite plan, DataFusion scanning parquet, deleted rows
 * subtracted by the live-docs bitmap). On false it falls through to {@code PPLService}, the
 * standard opensearch-sql engine, which lowers {@code stats count()} to an ordinary OpenSearch
 * aggregation over Lucene. The baseline is therefore a fully independent implementation, not a
 * differently-configured copy of the same one — which is what makes the comparison worth running.
 * {@link #assertEnginesDiffer()} checks the split actually happened.
 *
 * <p>Requires three things from the {@code integTestCompositeLuceneParity} cluster:
 * {@code cluster.pluggable.dataformat} unset so PPL routing is decided per index; merges off so
 * deleted rows stay on disk and the live-docs filter is load-bearing; and {@code dsl-query-executor}
 * not installed, because its {@code SearchActionFilter} redirects every {@code SearchAction} into
 * the analytics path regardless of index, which cannot read a non-pluggable shard
 * ({@code acquireReader} is unsupported on {@code EngineBackedIndexer}).
 *
 * <p>Every assertion is three-way — composite, plain Lucene, and a count computed in Java from
 * the same rules that generated the corpus. A two-way comparison would pass if both sides were
 * wrong in the same way, which is exactly what a routing mistake produces.
 */
public class CompositeLuceneParityIT extends AnalyticsRestTestCase {

    private static final String COMPOSITE_INDEX = "parity_composite";
    private static final String LUCENE_INDEX = "parity_lucene";

    private static final int DOC_COUNT = 2000;
    private static final int SHARDS = 2;

    private static final String[] NON_RU_COUNTRIES = { "US", "BR", "DE", "FR", "IN", "CN", "GB" };

    /** A query shape from the test plan: the {@code where} clause plus the Java oracle for it. */
    private record Query(String label, String whereClause, IntPredicate oracle) {}

    /** Cumulative delete density checkpoint. */
    private record Stage(String label, int deletedDocs) {}

    // Corpus rules. Each predicate is driven by a distinct modulus so every AND/OR
    // intersection below is non-empty at DOC_COUNT docs.
    private static boolean regionIs229(int i) { return i % 5 == 0; }
    private static boolean counterIs229(int i) { return i % 7 == 0; }
    private static boolean ageOver30(int i) { return i % 78 > 30; }
    private static boolean incomeOver10(int i) { return i % 20 > 10; }
    private static boolean titleHasGoogle(int i) { return i % 3 == 0; }
    private static boolean titleHasRu(int i) { return i % 11 == 0; }
    private static boolean refererHasHttp(int i) { return i % 2 == 0; }
    private static boolean countryIsRu(int i) { return i % 13 == 0; }

    private static final List<Query> QUERIES = List.of(
        new Query(
            "pure-datafusion, match-all required: RegionID = 229",
            "RegionID = 229",
            CompositeLuceneParityIT::regionIs229
        ),
        new Query(
            "pure-datafusion, match-all required: RegionID = 229 AND Age > 30",
            "RegionID = 229 AND Age > 30",
            i -> regionIs229(i) && ageOver30(i)
        ),
        new Query(
            "pure-datafusion, match-all required: RegionID = 229 OR Age > 30",
            "RegionID = 229 OR Age > 30",
            i -> regionIs229(i) || ageOver30(i)
        ),
        new Query(
            "single collector, no match-all: match(Title, 'google')",
            "match(Title, 'google')",
            CompositeLuceneParityIT::titleHasGoogle
        ),
        new Query(
            "single collector, no match-all: match(Title, 'google') AND CounterID = 229",
            "match(Title, 'google') AND CounterID = 229",
            i -> titleHasGoogle(i) && counterIs229(i)
        ),
        new Query(
            "single collector, no match-all: BrowserCountry = 'RU' AND CounterID = 229",
            "BrowserCountry = 'RU' AND CounterID = 229",
            i -> countryIsRu(i) && counterIs229(i)
        ),
        new Query(
            "tree, match-all required: match(Title, 'google') OR CounterID = 229",
            "match(Title, 'google') OR CounterID = 229",
            i -> titleHasGoogle(i) || counterIs229(i)
        ),
        new Query(
            "tree, no match-all: (match(Title, 'google') AND CounterID = 229) OR (match(Title, 'ru') AND Age > 30)",
            "(match(Title, 'google') AND CounterID = 229) OR (match(Title, 'ru') AND Age > 30)",
            i -> (titleHasGoogle(i) && counterIs229(i)) || (titleHasRu(i) && ageOver30(i))
        ),
        new Query(
            "tree, match-all required: three-way OR adding (match(Referer, 'http') OR Income > 10)",
            "(match(Title, 'google') AND CounterID = 229) OR (match(Title, 'ru') AND Age > 30)"
                + " OR (match(Referer, 'http') OR Income > 10)",
            i -> (titleHasGoogle(i) && counterIs229(i))
                || (titleHasRu(i) && ageOver30(i))
                || (refererHasHttp(i) || incomeOver10(i))
        )
    );

    private static final List<Stage> STAGES = List.of(
        new Stage("no deletes (control)", 0),
        new Stage("sparse 0.8%", 16),
        new Stage("dense 10%", 200),
        new Stage("dense 20%", 400),
        new Stage("dense 50%", 1000)
    );

    public void testParityAcrossDeleteDensities() throws Exception {
        createIndex(COMPOSITE_INDEX, true);
        createIndex(LUCENE_INDEX, false);
        ingest(COMPOSITE_INDEX);
        ingest(LUCENE_INDEX);

        assertEquals("composite doc count after ingest", DOC_COUNT, docCount(COMPOSITE_INDEX));
        assertEquals("lucene doc count after ingest", DOC_COUNT, docCount(LUCENE_INDEX));

        assertEnginesDiffer();

        List<Integer> deleteOrder = new ArrayList<>(DOC_COUNT);
        for (int i = 0; i < DOC_COUNT; i++) {
            deleteOrder.add(i);
        }
        Collections.shuffle(deleteOrder, random());

        List<String> failures = new ArrayList<>();
        int deletedSoFar = 0;

        for (Stage stage : STAGES) {
            if (stage.deletedDocs() > deletedSoFar) {
                List<Integer> batch = deleteOrder.subList(deletedSoFar, stage.deletedDocs());
                deleteDocs(COMPOSITE_INDEX, batch);
                deleteDocs(LUCENE_INDEX, batch);
                deletedSoFar = stage.deletedDocs();
            }

            boolean[] deleted = new boolean[DOC_COUNT];
            for (int i = 0; i < deletedSoFar; i++) {
                deleted[deleteOrder.get(i)] = true;
            }

            for (Query query : QUERIES) {
                long expected = oracleCount(query.oracle(), deleted);
                // A server-side failure on one side is recorded and the matrix continues: one broken
                // query shape must not hide the other 44 data points.
                Long composite = countOrRecord(COMPOSITE_INDEX, query, stage, failures);
                Long lucene = countOrRecord(LUCENE_INDEX, query, stage, failures);
                if (composite == null || lucene == null) {
                    continue;
                }

                if (composite != expected || lucene != expected) {
                    failures.add(
                        String.format(
                            "[%s] %s%n    expected=%d composite=%d lucene=%d",
                            stage.label(),
                            query.label(),
                            expected,
                            composite,
                            lucene
                        )
                    );
                } else if (stage.deletedDocs() == 0 && expected == 0) {
                    failures.add(
                        String.format(
                            "[%s] %s%n    matches no documents — literal is vacuous, the query "
                                + "would pass with delete filtering removed entirely",
                            stage.label(),
                            query.label()
                        )
                    );
                }
            }
        }

        if (failures.isEmpty() == false) {
            fail(failures.size() + " parity failure(s):" + System.lineSeparator() + String.join(System.lineSeparator(), failures));
        }
    }

    /** TEMP DIAGNOSTIC — remove before commit. Does a reader rebuilt over on-disk .liv files fix the query? */
    public void testRefreshFlushReopenLiveDocs() throws Exception {
        String ppl = "source=" + COMPOSITE_INDEX + " | where RegionID = 229 | stats count()";
        createIndex(COMPOSITE_INDEX, true);
        ingest(COMPOSITE_INDEX);

        StringBuilder report = new StringBuilder();
        report.append("phase=after-ingest expected=400 ").append(probe(ppl)).append(System.lineSeparator());

        List<Integer> batch = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            batch.add(i * 7 % DOC_COUNT);
        }
        deleteDocs(COMPOSITE_INDEX, batch);

        client().performRequest(new Request("POST", "/" + COMPOSITE_INDEX + "/_refresh"));
        flush(COMPOSITE_INDEX);
        report.append("phase=after-delete-refresh-flush expected=360 ").append(probe(ppl)).append(System.lineSeparator());

        client().performRequest(new Request("POST", "/" + COMPOSITE_INDEX + "/_close"));
        client().performRequest(new Request("POST", "/" + COMPOSITE_INDEX + "/_open"));
        Request health = new Request("GET", "/_cluster/health/" + COMPOSITE_INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "60s");
        client().performRequest(health);
        report.append("phase=after-close-open expected=360 ").append(probe(ppl)).append(System.lineSeparator());

        logger.info("REOPEN_PROBE_START{}{}REOPEN_PROBE_END", System.lineSeparator(), report);
    }

    private String probe(String ppl) throws Exception {
        try {
            return "count=" + pplCount(ppl);
        } catch (Exception e) {
            return "FAILED: " + e.getMessage().replaceAll("\\s+", " ");
        }
    }

    /**
     * The whole comparison rests on the two indexes being executed by different engines.
     * {@code TransportPPLQueryAction} routes to {@code RestUnifiedQueryAction} (analytics /
     * DataFusion) only when {@code isAnalyticsIndex} is true, and otherwise falls through to
     * {@code PPLService} — the standard opensearch-sql path that lowers the query to an
     * ordinary OpenSearch DSL aggregation. Nothing else in the test would notice if both
     * indexes went to the same engine: the composite side would "pass" by being the baseline.
     * Comparing the two explain plans catches that, and prints both when it fires so the
     * routing decision is visible rather than inferred.
     */
    private void assertEnginesDiffer() throws Exception {
        String compositePlan = explainPlan(COMPOSITE_INDEX);
        String lucenePlan = explainPlan(LUCENE_INDEX);
        logger.info("composite plan: {}", compositePlan);
        logger.info("lucene plan: {}", lucenePlan);

        assertNotEquals("identical plans mean both indexes hit the same engine", compositePlan, lucenePlan);

        // Vanilla side: proof the count is a server-side DSL aggregation. PushDownContext carries
        // AGGREGATION and the request is size:0 with track_total_hits, so nothing is counted
        // client-side and the 10000-row QUERY_SIZE_LIMIT cannot cap the result.
        assertTrue("vanilla plan should scan via Calcite's OpenSearch index scan: " + lucenePlan,
            lucenePlan.contains("CalciteLogicalIndexScan"));
        assertTrue("vanilla plan should push the aggregation into a DSL request: " + lucenePlan,
            lucenePlan.contains("OpenSearchRequestBuilder") && lucenePlan.contains("AGGREGATION->"));
        assertTrue("vanilla DSL request should be size:0 so count() comes from the aggregation: " + lucenePlan,
            lucenePlan.contains("\\\"size\\\":0"));

        // Composite side: the analytics engine leaves the scan as a bare LogicalTableScan here and
        // emits no physical plan, so this endpoint cannot show what DataFusion will actually run.
        // Use POST /_analytics/ppl/_explain when the analytics physical plan is needed.
        assertTrue("composite plan should be the unlowered analytics scan: " + compositePlan,
            compositePlan.contains("LogicalTableScan"));
    }

    /** Explain text with the index name normalized out, so only the engine's plan shape remains. */
    private String explainPlan(String index) throws Exception {
        Request request = new Request("POST", "/_plugins/_ppl/_explain");
        request.setJsonEntity("{\"query\": \"" + escapeJson("source=" + index + " | where RegionID = 229 | stats count()") + "\"}");
        Response response = client().performRequest(request);
        assertEquals("explain on " + index + " expected HTTP 200", 200, response.getStatusLine().getStatusCode());
        String body;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
            body = reader.lines().collect(Collectors.joining(System.lineSeparator()));
        }
        return body.replace(index, "<index>");
    }

    private void createIndex(String index, boolean pluggable) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (ResponseException ignored) {}

        StringBuilder settings = new StringBuilder();
        settings.append("\"number_of_shards\": ").append(SHARDS).append(", ");
        settings.append("\"number_of_replicas\": 0");
        if (pluggable) {
            settings.append(", \"index.pluggable.dataformat.enabled\": true");
            settings.append(", \"index.pluggable.dataformat\": \"composite\"");
            settings.append(", \"index.composite.primary_data_format\": \"parquet\"");
            settings.append(", \"index.composite.secondary_data_formats\": [\"lucene\"]");
            // Inherits true from the dataformat flag and is Final, so it must be set here.
            settings.append(", \"index.append_only.enabled\": false");
        } else {
            settings.append(", \"index.pluggable.dataformat.enabled\": false");
        }

        String body = "{"
            + "\"settings\": {" + settings + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"RegionID\": { \"type\": \"integer\" },"
            + "    \"CounterID\": { \"type\": \"integer\" },"
            + "    \"Age\": { \"type\": \"short\" },"
            + "    \"Income\": { \"type\": \"short\" },"
            + "    \"Title\": { \"type\": \"text\" },"
            + "    \"Referer\": { \"type\": \"text\" },"
            + "    \"BrowserCountry\": { \"type\": \"keyword\" }"
            + "  }"
            + "}"
            + "}";

        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "Create index " + index);
        assertEquals("index " + index + " acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + index);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "60s");
        client().performRequest(health);
    }

    private void ingest(String index) throws Exception {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < DOC_COUNT; i++) {
            bulk.append("{\"index\": {\"_id\": \"").append(i).append("\"}}\n");
            bulk.append(document(i)).append('\n');
        }
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.setJsonEntity(bulk.toString());
        request.addParameter("refresh", "true");
        Map<String, Object> response = assertOkAndParse(client().performRequest(request), "Bulk index " + index);
        assertEquals("bulk into " + index + " had failures", Boolean.FALSE, response.get("errors"));
        flush(index);
    }

    private static String document(int i) {
        StringBuilder title = new StringBuilder("page");
        if (titleHasGoogle(i)) {
            title.append(" google");
        }
        if (titleHasRu(i)) {
            title.append(" ru");
        }
        title.append(" report");

        String scheme = refererHasHttp(i) ? "http" : "ftp";
        String country = countryIsRu(i) ? "RU" : NON_RU_COUNTRIES[i % NON_RU_COUNTRIES.length];

        return "{"
            + "\"RegionID\": " + (regionIs229(i) ? 229 : 100 + (i % 97)) + ","
            + "\"CounterID\": " + (counterIs229(i) ? 229 : 300 + (i % 89)) + ","
            + "\"Age\": " + (i % 78) + ","
            + "\"Income\": " + (i % 20) + ","
            + "\"Title\": \"" + title + "\","
            + "\"Referer\": \"" + scheme + "://host" + i + "/path\","
            + "\"BrowserCountry\": \"" + country + "\""
            + "}";
    }

    private void deleteDocs(String index, List<Integer> ids) throws Exception {
        StringBuilder bulk = new StringBuilder();
        for (int id : ids) {
            bulk.append("{\"delete\": {\"_id\": \"").append(id).append("\"}}\n");
        }
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.setJsonEntity(bulk.toString());
        request.addParameter("refresh", "true");
        Map<String, Object> response = assertOkAndParse(client().performRequest(request), "Bulk delete " + index);
        assertEquals("bulk delete on " + index + " had failures", Boolean.FALSE, response.get("errors"));
        flush(index);
    }

    /** Both engines read from disk, so a refresh alone is not enough. */
    private void flush(String index) throws Exception {
        client().performRequest(new Request("POST", "/" + index + "/_flush?force=true"));
    }

    /** Runs one side of a comparison; on a server-side error records it and returns null. */
    private Long countOrRecord(String index, Query query, Stage stage, List<String> failures) throws Exception {
        try {
            return count(index, query.whereClause());
        } catch (ResponseException e) {
            failures.add(
                String.format(
                    "[%s] %s%n    %s failed server-side: %s",
                    stage.label(),
                    query.label(),
                    index,
                    e.getMessage().replaceAll("\\s+", " ")
                )
            );
            return null;
        }
    }

    private long count(String index, String whereClause) throws Exception {
        return pplCount("source=" + index + " | where " + whereClause + " | stats count()");
    }

    private long pplCount(String ppl) throws Exception {
        Map<String, Object> result = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("datarows must not be null for: " + ppl, rows);
        assertEquals("scalar aggregate must return exactly 1 row for: " + ppl, 1, rows.size());
        return ((Number) rows.get(0).get(0)).longValue();
    }

    /**
     * Unfiltered count. Goes through PPL, not {@code _search}: DSL search cannot serve both
     * index types on one cluster — the composite index needs {@code dsl-query-executor}'s
     * {@code SearchActionFilter} to reach the analytics engine, and that filter is unconditional,
     * so installing it breaks the plain-Lucene index instead.
     */
    private long docCount(String index) throws Exception {
        return pplCount("source=" + index + " | stats count()");
    }

    private static long oracleCount(IntPredicate matches, boolean[] deleted) {
        long count = 0;
        for (int i = 0; i < DOC_COUNT; i++) {
            if (deleted[i] == false && matches.test(i)) {
                count++;
            }
        }
        return count;
    }
}
