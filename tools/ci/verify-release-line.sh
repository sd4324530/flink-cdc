#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with the
#  License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

# Verifies the release-line integrity of the per-Flink-version artifacts (see
# FLIP-per-flink-version-modules.md and flink-cdc-flink/DIVERGENCE-INVENTORY.md):
#
#   1. no duplicate class entries in the shaded dist jar / MySQL sql-connector fat jar;
#   2. version-module marker classes match the line (1.20-only vs 2.2-only classes);
#   3. ecosystem dependencies resolve to the line's paimon/iceberg/fluss/kafka builds;
#   4. (optional) class-set parity of the dist jar against a legacy -Pflink2 baseline dist
#      tarball. Parity is a dist-level assertion by design: the connector fat jars legitimately
#      differ from the legacy build (line-specific shaded-guava version; the version module is
#      now bundled), which is why only the dist jar is compared.
#
# Usage:
#   tools/ci/verify-release-line.sh <flink1.20|flink2.2> [options]
#
# Options:
#   --repo-root DIR      repository root (default: inferred from this script's location)
#   --dist-tar FILE      built dist tarball
#                        (default: <root>/flink-cdc-dist/target/flink-cdc-*-flink<fv>-bin.tar.gz)
#   --sql-jar FILE       built MySQL sql-connector fat jar (default: auto-detected)
#   --baseline-dist FILE legacy -Pflink2 dist tarball for dist class-set parity (optional)
#   --skip-ecosystem     skip the dependency:list ecosystem assertion (offline environments)

set -uo pipefail

fail() {
    echo "[FAIL] $*"
    FAILURES=$((FAILURES + 1))
}

pass() {
    echo "[ok] $*"
}

LINE="${1:-}"
if [ "${LINE}" != "flink1.20" ] && [ "${LINE}" != "flink2.2" ]; then
    echo "Usage: $0 <flink1.20|flink2.2> [options]" >&2
    exit 2
fi
shift

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DIST_TAR=""
SQL_JAR=""
BASELINE_DIST=""
SKIP_ECOSYSTEM="false"

while [ $# -gt 0 ]; do
    case "$1" in
        --repo-root) REPO_ROOT="$2"; shift 2 ;;
        --dist-tar) DIST_TAR="$2"; shift 2 ;;
        --sql-jar) SQL_JAR="$2"; shift 2 ;;
        --baseline-dist) BASELINE_DIST="$2"; shift 2 ;;
        --skip-ecosystem) SKIP_ECOSYSTEM="true"; shift ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

FAILURES=0
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

case "${LINE}" in
    flink1.20)
        PROFILE="release-flink1.20"
        EXPECT_PRESENT=("org/apache/flink/streaming/util/RestartStrategyUtils.class")
        EXPECT_ABSENT=("org/apache/flink/streaming/api/functions/sink/RichSinkFunction.class")
        PAIMON_FLINK="paimon-flink-1.20"
        ICEBERG_FLINK="iceberg-flink-runtime-1.20"
        FLUSS_FLINK="fluss-flink-1.20"
        KAFKA_VERSION="flink-connector-kafka:jar:3.3.0-1.20"
        ES_CONNECTOR="flink-sql-connector-elasticsearch6:jar:3.1.0-1.20"
        ;;
    flink2.2)
        PROFILE="release-flink2.2"
        EXPECT_PRESENT=(
            "org/apache/flink/streaming/api/functions/sink/RichSinkFunction.class"
            "org/apache/flink/table/api/TableSchema.class")
        EXPECT_ABSENT=("org/apache/flink/streaming/util/RestartStrategyUtils.class")
        PAIMON_FLINK="paimon-flink-2.1"
        ICEBERG_FLINK="iceberg-flink-runtime-2.0"
        FLUSS_FLINK="fluss-flink-2.2"
        KAFKA_VERSION="flink-connector-kafka:jar:4.0.1-2.0"
        ES_CONNECTOR="flink-sql-connector-elasticsearch6:jar:4.0.0-2.0"
        ;;
esac

# ---------------------------------------------------------------------------
# Locate artifacts
# ---------------------------------------------------------------------------
if [ -z "${DIST_TAR}" ]; then
    DIST_TAR="$(ls -1 "${REPO_ROOT}"/flink-cdc-dist/target/flink-cdc-*-flink*-bin.tar.gz 2>/dev/null | head -1 || true)"
fi
if [ -z "${DIST_TAR}" ] || [ ! -f "${DIST_TAR}" ]; then
    echo "Dist tarball not found. Build it first, e.g.:" >&2
    echo "  mvn package -P${PROFILE} -pl flink-cdc-dist -am -DskipTests" >&2
    exit 2
fi
echo "Dist tarball: ${DIST_TAR}"

if [ -z "${SQL_JAR}" ]; then
    SQL_JAR="$(ls -1 "${REPO_ROOT}"/flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mysql-cdc/target/flink-sql-connector-mysql-cdc-*.jar 2>/dev/null | grep -v -- '-tests' | head -1 || true)"
fi
if [ -z "${SQL_JAR}" ] || [ ! -f "${SQL_JAR}" ]; then
    echo "MySQL sql-connector fat jar not found. Build it first, e.g.:" >&2
    echo "  mvn package -P${PROFILE} -pl flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mysql-cdc -am -DskipTests" >&2
    exit 2
fi
echo "MySQL sql-connector fat jar: ${SQL_JAR}"

# Extract the shaded dist jar from the tarball (lib/flink-cdc-dist-<version>.jar).
mkdir -p "${WORK}/dist"
tar -xzf "${DIST_TAR}" -C "${WORK}/dist"
DIST_JAR="$(find "${WORK}/dist" -name 'flink-cdc-dist-*.jar' | head -1)"
if [ -z "${DIST_JAR}" ]; then
    echo "No flink-cdc-dist jar inside the tarball: ${DIST_TAR}" >&2
    exit 2
fi
echo "Dist jar (from tarball): ${DIST_JAR}"

# ---------------------------------------------------------------------------
# Assertion 1: no duplicate class entries in the shaded jars
# ---------------------------------------------------------------------------
for jar in "${DIST_JAR}" "${SQL_JAR}"; do
    dups="$(jar tf "${jar}" | grep '\.class$' | sort | uniq -d || true)"
    if [ -n "${dups}" ]; then
        fail "duplicate class entries in ${jar}, e.g. $(echo "${dups}" | head -3 | tr '\n' ' ')"
    else
        pass "no duplicate classes in $(basename "${jar}")"
    fi
done

# ---------------------------------------------------------------------------
# Assertion 2: version-module marker classes match the line
# ---------------------------------------------------------------------------
for marker in "${EXPECT_PRESENT[@]}"; do
    count="$(jar tf "${SQL_JAR}" | grep -cFx "${marker}" || true)"
    if [ "${count}" -eq 1 ]; then
        pass "marker present exactly once: ${marker}"
    else
        fail "expected marker ${marker} exactly once in $(basename "${SQL_JAR}"), found ${count}"
    fi
done
for marker in "${EXPECT_ABSENT[@]}"; do
    count="$(jar tf "${SQL_JAR}" | grep -cFx "${marker}" || true)"
    if [ "${count}" -eq 0 ]; then
        pass "marker absent as expected: ${marker}"
    else
        fail "expected marker ${marker} absent from $(basename "${SQL_JAR}"), found ${count}"
    fi
done

# ---------------------------------------------------------------------------
# Assertion 3: ecosystem dependencies resolve to the line's builds
# ---------------------------------------------------------------------------
if [ "${SKIP_ECOSYSTEM}" = "true" ]; then
    echo "[skip] ecosystem dependency assertion (--skip-ecosystem)"
else
    PIPELINE_DIR="flink-cdc-connect/flink-cdc-pipeline-connectors"
    declare -A EXPECTED=(
        [paimon]="${PAIMON_FLINK}"
        [iceberg]="${ICEBERG_FLINK}"
        [fluss]="${FLUSS_FLINK}"
        [kafka]="${KAFKA_VERSION}"
        [elasticsearch]="${ES_CONNECTOR}"
    )
    # The ES module guards exactly the failure mode that actually happened during Phase 3:
    # a module-level property can shadow the profile's ecosystem override via Maven's
    # nearest-definition rule. Hudi/Doris intentionally keep their 1.20-line ecosystem
    # builds on both lines for now (e2e-verified behavior).
    for connector in paimon iceberg fluss kafka elasticsearch; do
        module="flink-cdc-pipeline-connector-${connector}"
        out="${WORK}/deps-${connector}.txt"
        # Note: -pl needs the module path relative to the repo root (absolute paths fail).
        if ! (cd "${REPO_ROOT}" && mvn -B -q dependency:list -P"${PROFILE}" \
                -pl "${PIPELINE_DIR}/${module}" -DoutputFile="${out}" >/dev/null 2>&1); then
            fail "dependency:list failed for ${module} (-P${PROFILE})"
            continue
        fi
        if grep -aq "${EXPECTED[${connector}]}" "${out}"; then
            pass "${module} resolves ${EXPECTED[${connector}]}"
        else
            fail "${module} does not resolve ${EXPECTED[${connector}]} under -P${PROFILE}"
        fi
    done
fi

# ---------------------------------------------------------------------------
# Assertion 4 (optional): class-set parity against legacy -Pflink2 baselines
# ---------------------------------------------------------------------------
parity_check() {
    local new_jar="$1" baseline_artifact="$2" label="$3"
    local baseline_jar="${baseline_artifact}"
    if [[ "${baseline_artifact}" == *.tar.gz ]]; then
        mkdir -p "${WORK}/baseline-${label}"
        tar -xzf "${baseline_artifact}" -C "${WORK}/baseline-${label}"
        baseline_jar="$(find "${WORK}/baseline-${label}" -name 'flink-cdc-dist-*.jar' | head -1)"
        if [ -z "${baseline_jar}" ]; then
            fail "no flink-cdc-dist jar inside baseline tarball ${baseline_artifact}"
            return
        fi
    fi
    jar tf "${new_jar}" | grep '\.class$' | sort > "${WORK}/new-${label}.classes"
    jar tf "${baseline_jar}" | grep '\.class$' | sort > "${WORK}/base-${label}.classes"
    if diff -q "${WORK}/new-${label}.classes" "${WORK}/base-${label}.classes" >/dev/null; then
        pass "class-set parity (${label}): diff = 0 vs $(basename "${baseline_artifact}")"
    else
        fail "class-set parity (${label}) broken vs ${baseline_artifact}; first diffs:"
        diff "${WORK}/new-${label}.classes" "${WORK}/base-${label}.classes" | head -10 | sed 's/^/    /'
    fi
}

if [ -n "${BASELINE_DIST}" ]; then
    if [ -f "${BASELINE_DIST}" ]; then
        parity_check "${DIST_JAR}" "${BASELINE_DIST}" "dist"
    else
        fail "baseline dist artifact not found: ${BASELINE_DIST}"
    fi
else
    echo "[skip] class-set parity (dist): no --baseline-dist given"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo "------------------------------------------------------------------------"
if [ "${FAILURES}" -eq 0 ]; then
    echo "RELEASE-LINE ${LINE}: ALL ASSERTIONS PASSED"
    exit 0
else
    echo "RELEASE-LINE ${LINE}: ${FAILURES} ASSERTION(S) FAILED"
    exit 1
fi
