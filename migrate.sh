#!/usr/bin/env bash

# prepare migration environment

sudo timedatectl set-timezone UTC  # this is important for the proper functioning of orc-tools
sudo apt update
sudo apt install default-jre awscli postgresql-client
wget https://repo1.maven.org/maven2/org/apache/orc/orc-tools/1.6.7/orc-tools-1.6.7-uber.jar

# export postgres data as json file

psql -d wh_restore -f -a ./export_postgres_data.sql

# python script that prepares the files that will be migrated to Athena

python3 ./prepare_migration_files.py

# orc-tools fails if there are existing ORC files

rm *.orc

# export web_historian_behavior_data.orc

java -jar ./orc-tools-1.6.7-uber.jar convert ./migration_files/files/web_historian_behavior_data.json -o ./migration_files/files/web_historian_behavior_data.orc -s 'struct<id:int,source:string,generator:string,created_utc:timestamp,created_str:string,generated_at:string,recorded_utc:timestamp,recorded_str:string,properties:struct<passive_data_metadata:struct<source:string,generator:string,encrypted_transmission:boolean,generator_id:string,pdk_timestamp:decimal(16,7)>,web_historian_server:struct<visits:int,domains:int,searches:int>,source_info:struct<visits:int,domains:int,searches:int,source:string>>,generator_identifier:string,secondary_identifier:string,user_agent:string,server_generated:boolean,generator_definition_id:int,source_reference_id:int>' -t "yyyy-MM-dd HH:mm:ss.n"
aws s3 cp ./migration_files/files/web_historian_behavior_data.orc s3://web-historian-eu/web_historian_behavior_data/web_historian_behavior_data.orc

# export pdk_app_event.org after fixing 3 known inconsistencies in the JSON structure of pdk_app_events

grep -o ', "search_count": \[null\]' ./migration_files/files/pdk_app_event.json | wc -l
sed -i 's/, "search_count": \[null\]//g' ./migration_files/files/pdk_app_event.json
grep -o ', "search_count": \[null\]' ./migration_files/files/pdk_app_event.json | wc -l

grep -o '"count": \[' ./migration_files/files/pdk_app_event.json | wc -l
sed -i 's/"count": \[/"count": /g' ./migration_files/files/pdk_app_event.json
grep -o '"count": \[' ./migration_files/files/pdk_app_event.json | wc -l

grep -o '\],' ./migration_files/files/pdk_app_event.json | wc -l
sed -i 's/\],/,/g' ./migration_files/files/pdk_app_event.json
grep -o '\],' ./migration_files/files/pdk_app_event.json | wc -l

java -jar ./orc-tools-1.6.7-uber.jar convert ./migration_files/files/pdk_app_event.json -o ./migration_files/files/pdk_app_event.orc -s 'struct<id:int,source:string,generator:string,created_utc:timestamp,created_str:string,generated_at:string,recorded_utc:timestamp,recorded_str:string,properties:struct<event_name:string,event_details:struct<step:int,session_id:string,count:int,domain_count:int,study:string,search_term_count:int>,properties_date:decimal(16,4),passive_data_metadata:struct<source:string,generator:string,generator_id:string,pdk_timestamp:decimal(16,7),encrypted_transmission:boolean>>,generator_identifier:string,secondary_identifier:string,user_agent:string,server_generated:boolean,generator_definition_id:int,source_reference_id:int>' -t "yyyy-MM-dd HH:mm:ss.n"
aws s3 cp ./migration_files/files/pdk_app_event.orc s3://web-historian-eu/pdk_app_event/pdk_app_event.orc

# export all the web_historian files

web_historian_folder=`du ./migration_files/files/web_historian/* | sort -rn | cut -f 2`
echo `date`
for dir in $web_historian_folder     # list directories in the form "/tmp/dirname/"
do
  current_week=${dir##*/}
  echo "${current_week}"
  mkdir -p "./migration_files/orc_files/web_historian/${current_week}" && \
  java -jar ./orc-tools-1.6.6-uber.jar convert "./migration_files/files/web_historian/${current_week}/web_historian_data.json" -o "./migration_files/orc_files/web_historian/${current_week}/web_historian_data.orc" -s 'struct<id:int,source:string,generator:string,created_utc:timestamp,created_str:string,generated_at:string,recorded_utc:timestamp,recorded_str:string,properties:struct<id:string,url:string,title:string,domain:string,visitId:string,transType:string,refVisitId:string,searchTerms:string,properties_date:decimal(16,4),protocol:string,passive_data_metadata:struct<source:string,generator:string,generator_id:string,pdk_timestamp:decimal(16,7),encrypted_transmission:boolean>>,generator_identifier:string,secondary_identifier:string,user_agent:string,server_generated:boolean,generator_definition_id:int,source_reference_id:int>' -t "yyyy-MM-dd HH:mm:ss.n" 2>&1 | tee ./migration_files/orc_output.log && \
  aws s3 sync ./migration_files/orc_files/web_historian s3://web-historian-eu/web_historian --exclude="*.crc"
  echo `date`
done

