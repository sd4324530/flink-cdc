#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

##
## Variables with defaults (if not overwritten by environment)
##
MVN=${MVN:-mvn}
CUSTOM_OPTIONS=${CUSTOM_OPTIONS:-}

# Note on per-line releases (per-Flink-version modules, FLIP 4.8):
# Deploying the 2.2 line must (a) run with -Prelease-flink2.2 so the ecosystem versions and
# the 2.2 version module are picked up, and (b) rewrite the thin-jar POMs' dependency on the
# baseline version module (flink-cdc-flink-1.20) to the line's version module
# (flink-cdc-flink-2.2) before staging, because main-module POMs always carry the baseline
# dependency. The versions-maven-plugin stamping below handles the version string; the
# baseline->line module swap still needs to be added here when the release tooling switches
# to the per-line profiles.

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should always exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

###########################

function get_pom_version {
  set +u
  echo $(${MVN} help:evaluate -Dexpression="project.version" -q -DforceStdout)
  set -u
}

function set_pom_version {
  new_version=$1

  ${MVN} org.codehaus.mojo:versions-maven-plugin:2.8.1:set -DnewVersion=${new_version} -DgenerateBackupPoms=false --quiet
}

function set_revision_property {
  new_revision=$1

  ${MVN} org.codehaus.mojo:versions-maven-plugin:2.8.1:set-property -Dproperty=revision -DnewVersion=${new_revision} -DgenerateBackupPoms=false --quiet
}

###########################

cd ${PROJECT_ROOT}

# Build Maven options
MVN_OPTIONS="-Papache-release -DskipTests -DretryFailedDeploymentCount=10"

# If FLINK_VERSION is set, pass it to Maven
if [ -n "${FLINK_VERSION:-}" ]; then
    echo "Using Flink version: ${FLINK_VERSION}"

    # Update project version: RELEASE_VERSION-FLINK_VERSION
    current_version=$(get_pom_version)
    echo "Current version: ${current_version}"

    if [ -z "${RELEASE_VERSION:-}" ]; then
        echo "ERROR: RELEASE_VERSION environment variable is required"
        exit 1
    fi
    new_version="${RELEASE_VERSION}-${FLINK_VERSION}"

    # Skip if version already matches RELEASE_VERSION-FLINK_VERSION
    if [[ "${current_version}" == "${new_version}" ]]; then
        echo "Version already is ${new_version}, skipping version update"
    else
        echo "Updating version to: ${new_version}"
        set_pom_version "${new_version}"
        set_revision_property "${new_version}"
    fi

    # Use the per-line release profile for non-baseline lines (e.g. -Prelease-flink2.2).
    # Main modules are always compiled against the 1.20 baseline; the profile selects the
    # version module and ecosystem versions bound into the line's artifacts. Normalize
    # FLINK_VERSION to major.minor so "2.2.0" maps to the existing release-flink2.2 profile.
    if [[ "${FLINK_VERSION}" != 1* ]]; then
        RELEASE_LINE=$(echo "${FLINK_VERSION}" | cut -d. -f1,2)
        echo "Enabling release-flink${RELEASE_LINE} profile"
        MVN_OPTIONS="${MVN_OPTIONS} -Prelease-flink${RELEASE_LINE}"
    fi
fi

echo "Deploying to repository.apache.org"
${MVN} clean deploy ${MVN_OPTIONS} ${CUSTOM_OPTIONS}

cd ${CURR_DIR}