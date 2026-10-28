/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc;

/**
 * Phase 2 rerun-suite manifest. The 2.2 version module reruns core suites of the main modules
 * against Flink 2.2 by extending the package-private test classes shipped in their test-jars (spec
 * D8).
 *
 * <p>Rerun list (first conservative cut, extend per release):
 *
 * <ol>
 *   <li>{@code
 *       org.apache.flink.cdc.runtime.operators.transform.RerunTransformExpressionCompilerTest} —
 *       transform expression compiler (flink-cdc-runtime)
 *   <li>{@code org.apache.flink.cdc.runtime.operators.transform.RerunPreTransformOperatorTest} —
 *       pre-transform operator semantics (flink-cdc-runtime)
 *   <li>{@code org.apache.flink.cdc.connectors.values.source.RerunValuesDataSourceITCase} —
 *       pipeline smoke test on MiniCluster (flink-cdc-pipeline-connector-values)
 * </ol>
 *
 * <p>Plus {@code AdapterOverrideAssertionTest}, which proves this module's adapter classes win the
 * test classpath over the 1.20 module jar that arrives transitively via the main modules (runtime →
 * common → flink-cdc-flink-1.20).
 *
 * <p>Deferred: the {@code NewlyAddedTableITCase} reruns (mysql, and the postgres/oracle/mongodb
 * siblings). Their 2.2-line verification ran on the connector modules themselves under the legacy
 * {@code flink2} profile before that mechanism was removed, re-run during Phase 3 as one-off triage
 * channels with the same finding for all four suites: mysql 6/21, postgres 6/20, oracle 6/20 and
 * mongodb 7/20 failures sharing ONE signature — exactly one missing row per newly-added
 * table/collection per discovery-triggered restart (the first row's 'China' -&gt; 'CHINA' UPDATE,
 * id 416874195632735147, is lost; binlog/WAL/redo/oplog events inside the restart window are
 * dropped under Flink 2.2's restart semantics). The uniform signature across four base-framework
 * connectors localizes the root cause to the shared newly-added-table restart path in
 * flink-cdc-base interacting with Flink 2.2 runtime semantics — tracked by JIRA FLINK-xxxxx (number
 * to be assigned). A CDC-side fix would land in the base hot path and has no standing 2.2 unit-test
 * channel to regress against (a bridge inside this module cannot host the connector suites together
 * with flink-cdc-runtime on one classpath: runtime's Calcite 1.32 and the Calcite shaded into
 * flink-table-planner 2.2 are mutually incompatible), so the suites are registered with
 * precise-signature notes instead of being hot-patched; each suite passes in full on the 1.20
 * baseline line. Post-Phase-3 the 2.2-line coverage for these suites is expected to come from
 * per-line e2e tests and from rebuilding a 2.2 unit-test channel (future work). See the per-suite
 * class comments for the triage evidence.
 */
final class RerunSuite {

    private RerunSuite() {}
}
