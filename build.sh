#!/usr/bin/env bash

set -e

HOME_PATH=$(cd `dirname $0`; pwd)

cd ${HOME_PATH}
version=`awk '/<ocsearch.version>[^<]+<\/ocsearch.version>/{gsub(/<ocsearch.version>|<\/ocsearch.version>/,"",$1);print $1;exit;}' pom.xml`
spark_version=1.6

mvn clean package -Dmaven.test.skip=true

if [ $? -ne 0 ]; then
   echo "Build failed..."
   exit 1
fi


rm -fr build


mkdir -p build/ocsearch/conf
mkdir -p build/ocsearch/logs
mkdir -p build/ocsearch/flume
mkdir -p build/ocsearch/expression
mkdir -p build/ocsearch/web
mkdir -p build/ocsearch/server

tar -xvf  apache-tomcat-8.5.14.tar -C build/ocsearch/server --strip-components 1

cp server.xml build/ocsearch/server/conf/

cp -r bin build/ocsearch


cp -r ocsearch-service/target/ocsearch-service-${version} build/ocsearch/server/webapps/ocsearch-service

cp ocsearch-service/target/ocsearch-service-${version}/WEB-INF/classes/*.* build/ocsearch/conf

cp ocsearch-flume/target/ocsearch-flume-${version}.jar build/ocsearch/flume/

cp ocsearch-expression/target/ocsearch-expression-${version}.jar build/ocsearch/expression/

tar -xzf web/target/web-${version}-bundle.tar.gz -C build/ocsearch/web

cd build

tar -czf ocsearch-${version}.tar.gz ocsearch

exit 0