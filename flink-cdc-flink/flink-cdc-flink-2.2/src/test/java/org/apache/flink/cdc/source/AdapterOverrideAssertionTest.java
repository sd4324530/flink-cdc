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

package org.apache.flink.cdc.source;

import org.apache.flink.cdc.runtime.operators.AsyncWaitOperatorAdapter;

import org.junit.jupiter.api.Test;

import java.security.CodeSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts that this module's adapter classes win the test classpath over the 1.20 version-module
 * jar that arrives transitively through the main modules (runtime → common → flink-cdc-flink-1.20).
 * If this regresses, the rerun suites would silently exercise 1.20 bytecode instead of the 2.2
 * adapters (Phase 2 Review Focus 5).
 */
class AdapterOverrideAssertionTest {

    @Test
    void runtimeContextAdapterLoadsFromThisModule() {
        CodeSource codeSource = RuntimeContextAdapter.class.getProtectionDomain().getCodeSource();
        assertThat(codeSource).isNotNull();
        assertThat(codeSource.getLocation().getPath())
                .contains("flink-cdc-flink-2.2")
                .doesNotContain("flink-cdc-flink-1.20");
    }

    @Test
    void asyncWaitOperatorAdapterLoadsFromThisModule() {
        CodeSource codeSource =
                AsyncWaitOperatorAdapter.class.getProtectionDomain().getCodeSource();
        assertThat(codeSource).isNotNull();
        assertThat(codeSource.getLocation().getPath())
                .contains("flink-cdc-flink-2.2")
                .doesNotContain("flink-cdc-flink-1.20");
    }
}
