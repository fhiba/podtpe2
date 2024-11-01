cd ../../client/target
tar -xzf tpe2-g12-client-1.0-SNAPSHOT-bin.tar.gz
cd tpe2-g12-client-1.0-SNAPSHOT
./query3.sh -Daddresses='127.0.0.1:5701' -Dcity=Sample -DinPath='C:\Users\juani\hazelcast\tpe2\' -DoutPath='C:\Users\juani\podtpe2\bash_tests\query3\' -Dn=2 -Dfrom=01/06/2021 -Dto=30/09/2021
