--Load the input file from HDFS
inputdial = LOAD 'hdfs://localhost:9000/user/dharani/episodeIV-dialogues.txt' USING PigStorage('\t') As (name:charArray,line:charArray);
ranked = RANK inputdial;
onlydial = FILTER ranked BY (rank_inputdial >2);
groupByName = GROUP onlydial BY name;
DUMP groupByName;
names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;
namesOrdered = ORDER names BY no_of_lines DESC;
STORE namesOrdered INTO 'hdfs://localhost:9000/user/dharani/episodeIVoutput' USING PigStorage('\t');

