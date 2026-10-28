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

# Adapter-contract assertion of the per-Flink-version modules (FLIP 4.4, spec D5): for every
# FQCN that exists in BOTH version modules (the adapter pairs), the public signatures must be
# identical — main modules are compiled against the 1.20 signatures and run on the 2.2 line.
#
# Note: javap signature parity is a NECESSARY condition for link compatibility, not evidence of
# behavioral equivalence — behavior is covered by the version-module tests, the rerun suites and
# the per-line e2e tests.
#
# Usage: tools/ci/verify-adapter-contract.sh [--repo-root DIR] [--jar-120 FILE] [--jar-220 FILE]

set -uo pipefail

fail() {
    echo "[FAIL] $*"
    FAILURES=$((FAILURES + 1))
}

pass() {
    echo "[ok] $*"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
JAR120=""
JAR220=""

while [ $# -gt 0 ]; do
    case "$1" in
        --repo-root) REPO_ROOT="$2"; shift 2 ;;
        --jar-120) JAR120="$2"; shift 2 ;;
        --jar-220) JAR220="$2"; shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

FAILURES=0
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

# Whitelisted public-signature deltas between the version modules (adapter FQCN -> reason).
# A whitelist entry suppresses ONLY the signature diff of the named class.
declare -A SIGNATURE_WHITELIST=(
    # The 2.2 AbstractStreamOperatorAdapter re-exposes a public setup(...) override; its 1.20
    # variant has no such member (the 1.20 base class carries it natively). Extra PUBLIC members
    # on the 2.2 side cannot break 1.20-compiled callers — the adapter contract only requires
    # that every 1.20-side member exists identically on the 2.2 side.
    ["org.apache.flink.cdc.runtime.operators.AbstractStreamOperatorAdapter"]="+public setup on 2.2 only"
    # The 1.20 AsyncSinkBaseAdapter shows an extra RAW restoreWriter(WriterInitContext,
    # Collection) member — a compiler-generated bridge for the generic override against the
    # 1.20-native StatefulSink hierarchy. The sources of the two adapters are member-identical;
    # on the 2.2 line the erased dispatch target is provided by the StatefulSink shadow itself.
    ["org.apache.flink.connector.base.sink.AsyncSinkBaseAdapter"]="+raw bridge on 1.20 only"
    # The 2.2 AsyncSinkWriterAdapter additionally exposes the 6-argument constructor that
    # Flink 2.2's AsyncSinkWriter added (batchCreator/requestBuffer injection). Additive public
    # member on the 2.2 side only — cannot break 1.20-compiled callers.
    ["org.apache.flink.connector.base.sink.writer.AsyncSinkWriterAdapter"]="+public ctor on 2.2 only"
    # TypeSerializerSnapshotAdapter IS the adaptation: Flink 2.2 changed the wrapped
    # TypeSerializerSnapshot contract shape, so the 2.2 variant re-declares the 1.20-shape
    # resolveSchemaCompatibility(TypeSerializer) as abstract (the member 1.20-compiled code
    # binds against) and adds a default bridging the 2.2-shape overload to it. Both extra
    # members are additive-by-design for 1.20-compiled callers.
    ["org.apache.flink.api.common.typeutils.TypeSerializerSnapshotAdapter"]="+2.2 adaptation members by design"
)

# ---------------------------------------------------------------------------
# Locate the version-module jars (module target first, then the local repo)
# ---------------------------------------------------------------------------
find_jar() {
    local module="$1"
    local found
    found="$(ls -1 "${REPO_ROOT}/flink-cdc-flink/${module}/target/"${module}-*.jar 2>/dev/null | grep -v -- '-tests' | head -1 || true)"
    if [ -z "${found}" ]; then
        found="$(find "${HOME}/.m2/repository/org/apache/flink/${module}" -name "${module}-*.jar" 2>/dev/null | grep -v -- '-tests' | sort | tail -1 || true)"
    fi
    echo "${found}"
}

if [ -z "${JAR120}" ]; then
    JAR120="$(find_jar flink-cdc-flink-1.20)"
fi
if [ -z "${JAR220}" ]; then
    JAR220="$(find_jar flink-cdc-flink-2.2)"
fi
if [ -z "${JAR120}" ] || [ ! -f "${JAR120}" ]; then
    echo "flink-cdc-flink-1.20 jar not found. Build it first:" >&2
    echo "  mvn install -pl flink-cdc-flink/flink-cdc-flink-1.20 -DskipTests" >&2
    exit 2
fi
if [ -z "${JAR220}" ] || [ ! -f "${JAR220}" ]; then
    echo "flink-cdc-flink-2.2 jar not found. Build it first:" >&2
    echo "  mvn install -pl flink-cdc-flink/flink-cdc-flink-2.2 -DskipTests" >&2
    exit 2
fi
echo "flink-cdc-flink-1.20 jar: ${JAR120}"
echo "flink-cdc-flink-2.2 jar: ${JAR220}"

# ---------------------------------------------------------------------------
# Class sets
# ---------------------------------------------------------------------------
jar tf "${JAR120}" | grep '\.class$' | grep -v 'module-info' | sed 's/\.class$//' | sort > "${WORK}/classes-120.txt"
jar tf "${JAR220}" | grep '\.class$' | grep -v 'module-info' | sed 's/\.class$//' | sort > "${WORK}/classes-220.txt"
# keep top-level classes only (inner classes belong to their outer class' signature), as
# dotted FQCNs for the signature pairing below
sed 's/\.class$//; s#/#.#g' "${WORK}/classes-120.txt" | grep -v '\$' > "${WORK}/top-120.txt"
sed 's/\.class$//; s#/#.#g' "${WORK}/classes-220.txt" | grep -v '\$' > "${WORK}/top-220.txt"
echo "1.20 module classes: $(wc -l < "${WORK}/top-120.txt")"
echo "2.2 module classes:  $(wc -l < "${WORK}/top-220.txt")"

# ---------------------------------------------------------------------------
# Assertion 2: same-FQCN adapter pairs expose identical public signatures
# ---------------------------------------------------------------------------
comm -12 "${WORK}/top-120.txt" "${WORK}/top-220.txt" | sed 's/\./\//g' > "${WORK}/pairs.txt"
PAIRS=$(wc -l < "${WORK}/pairs.txt")
echo "same-FQCN adapter pairs: ${PAIRS}"

normalize() {
    # strip the 'Compiled from' header, the class/interface DECLARATION line (the contract
    # covers constructors and methods; the declaration's supertype FQCNs legitimately differ —
    # e.g. StatefulSinkWriterAdapter extends StatefulSink$StatefulSinkWriter at 1.20 but the
    # top-level StatefulSinkWriter at 2.2, and main code cannot touch those confined supertypes
    # directly, which the ArchUnit guardrail enforces), blank lines and trailing semicolons;
    # sort member lines so declaration order does not matter
    grep -v '^Compiled from' | grep -v '{$' | sed 's/[[:space:]]*$//' | sed 's/;[[:space:]]*$//' | grep -v '^$' | sort
}

pairs_failed=0
while IFS= read -r cls; do
    fqcn="${cls//\//.}"
    javap -public -classpath "${JAR120}" "${fqcn}" 2>"${WORK}/err120.txt" | normalize > "${WORK}/sig-120.txt"
    javap -public -classpath "${JAR220}" "${fqcn}" 2>"${WORK}/err220.txt" | normalize > "${WORK}/sig-220.txt"
    if [ ! -s "${WORK}/sig-120.txt" ] || [ ! -s "${WORK}/sig-220.txt" ]; then
        fail "javap failed for ${fqcn}: $(head -1 "${WORK}/err120.txt") $(head -1 "${WORK}/err220.txt")"
        pairs_failed=$((pairs_failed + 1))
        continue
    fi
    if ! diff -q "${WORK}/sig-120.txt" "${WORK}/sig-220.txt" >/dev/null; then
        if [ -n "${SIGNATURE_WHITELIST[${fqcn}]:-}" ]; then
            pass "signature delta whitelisted for ${fqcn} (${SIGNATURE_WHITELIST[${fqcn}]})"
        else
            fail "public signature mismatch for ${fqcn}:"
            diff "${WORK}/sig-120.txt" "${WORK}/sig-220.txt" | head -10 | sed 's/^/    /'
            pairs_failed=$((pairs_failed + 1))
        fi
    fi
done < "${WORK}/pairs.txt"

if [ "${pairs_failed}" -eq 0 ]; then
    pass "all ${PAIRS} same-FQCN adapter pairs expose identical public signatures (modulo whitelist)"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo "------------------------------------------------------------------------"
if [ "${FAILURES}" -eq 0 ]; then
    echo "ADAPTER CONTRACT: ALL ASSERTIONS PASSED"
    exit 0
else
    echo "ADAPTER CONTRACT: ${FAILURES} ASSERTION(S) FAILED"
    exit 1
fi


