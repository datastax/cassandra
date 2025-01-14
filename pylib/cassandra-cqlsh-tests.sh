#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

################################
#
# Prep
#
################################

set -x

WORKSPACE=$1

[ "x${WORKSPACE}" != "x" ] || WORKSPACE="$(readlink -f $(dirname "$0")/..)"
[ "x${BUILD_DIR}" != "x" ] || BUILD_DIR="${WORKSPACE}/build"
[ "x${DIST_DIR}" != "x" ] || DIST_DIR="${WORKSPACE}/build"
[ "x${TMPDIR}" != "x" ] || TMPDIR="$(mktemp -d ${DIST_DIR}/run-python-dtest.XXXXXX)"

mkdir -p "${TMPDIR}"
export TMPDIR
export PYTHONIOENCODING="utf-8"
export PYTHONUNBUFFERED=true
export CASS_DRIVER_NO_EXTENSIONS=true
export CCM_MAX_HEAP_SIZE="2048M"
export CCM_HEAP_NEWSIZE="200M"
export CCM_CONFIG_DIR="${TMPDIR}/.ccm"
export NUM_TOKENS="16"
export CASSANDRA_DIR=${WORKSPACE}

if [[ -z "${version}" ]]; then
  version=$(grep 'property\s*name=\"base.version\"' ${CASSANDRA_DIR}/build.xml |sed -ne 's/.*value=\"\([^"]*\)\".*/\1/p')
fi

if [[ -z "${java_version}" ]]; then
  java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | sed 's/^1\.//g' | awk -F. '{print $1}')
fi

if [[ -z "${python_version}" ]]; then
  python_version="3.8"
  command -v python3 >/dev/null 2>&1 && python_version="$(python3 -V | awk '{print $2}' | awk -F'.' '{print $1"."$2}')"
fi

export TESTSUITE_NAME="cqlshlib.python${python_version}.jdk${java_version}"

pushd ${CASSANDRA_DIR} >/dev/null

# check project is already built. no cleaning is done, so jenkins unstash works, beware.
[[ -f "${BUILD_DIR}/dse-db-${version}.jar" ]] || [[ -f "${BUILD_DIR}/dse-db-${version}-SNAPSHOT.jar" ]] || { echo "Project must be built first. Use \`ant jar\`. Build directory is ${BUILD_DIR} with: $(ls ${BUILD_DIR})"; exit 1; }

# Set up venv with dtest dependencies
set -e # enable immediate exit if venv setup fails

# fresh virtualenv and test logs results everytime
rm -fr ${DIST_DIR}/venv ${DIST_DIR}/test/{html,output,logs}

if [ "$cython" = "yes" ]; then
  cython_suffix="cython"
else
  cython_suffix="no-cython"
fi

# re-use when possible the pre-installed virtualenv found in the cassandra-ubuntu2004_test docker image
virtualenv-clone ${BUILD_HOME}/${cython_suffix}/python${python_version} ${BUILD_DIR}/venv || virtualenv --python=python${python_version} ${BUILD_DIR}/venv
source ${BUILD_DIR}/venv/bin/activate

if [ "$cython" = "yes" ]; then
  pip install "Cython>=0.29.15,<3.0"
  unset CASS_DRIVER_NO_CYTHON
else
  export CASS_DRIVER_NO_CYTHON=true
fi

pip install --exists-action s -r ${CASSANDRA_DIR}/pylib/requirements.txt
pip freeze

if [ "$cython" = "yes" ]; then
    pushd pylib >/dev/null
    python setup.py build_ext --inplace
    popd >/dev/null
fi

TESTSUITE_NAME="${TESTSUITE_NAME}.${cython_suffix}.$(uname -m)"

################################
#
# Main
#
################################

ccm remove test || true # in case an old ccm cluster is left behind
ccm create test -n 1 --install-dir=${CASSANDRA_DIR}
ccm updateconf "enable_user_defined_functions: true"
ccm updateconf "cdc_enabled: true"

set +e # disable immediate exit from this point

ccm start --wait-for-binary-proto --jvm-version=${java_version}

pushd ${CASSANDRA_DIR}/pylib/cqlshlib/ >/dev/null

pytest --junitxml=${BUILD_DIR}/test/output/cqlshlib.xml
RETURN="$?"

# don't do inline sed for linux+mac compat
sed "s/testsuite name=\"pytest\"/testsuite name=\"${TESTSUITE_NAME}\"/g" ${BUILD_DIR}/test/output/cqlshlib.xml > /${TMPDIR}/cqlshlib.xml
cat /${TMPDIR}/cqlshlib.xml > ${BUILD_DIR}/test/output/cqlshlib.xml
sed "s/testcase classname=\"cqlshlib./testcase classname=\"${TESTSUITE_NAME}./g" ${BUILD_DIR}/test/output/cqlshlib.xml > /${TMPDIR}/cqlshlib.xml
cat /${TMPDIR}/cqlshlib.xml > ${BUILD_DIR}/test/output/cqlshlib.xml

# tar up any ccm logs for easy retrieval
if ls ${TMPDIR}/.ccm/test/*/logs/* &>/dev/null ; then
    mkdir -p ${DIST_DIR}/test/logs
    pushd ${TMPDIR}/.ccm/test >/dev/null
    tar -C ${TMPDIR}/.ccm/test -cJf ${DIST_DIR}/test/logs/ccm_logs.tar.xz */logs/*
    popd >/dev/null
fi

ccm remove

################################
#
# Clean
#
################################

rm -rf ${TMPDIR}
unset TMPDIR
deactivate
popd >/dev/null
popd >/dev/null

# circleci needs non-zero exit on failures, jenkins need zero exit to process the test failures
if ! command -v circleci >/dev/null 2>&1
then
    exit 0
else
    exit ${RETURN}
fi
