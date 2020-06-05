#!bin/bash
#sh test.sh > res 2>&1

python3 testSelUpDel.py
python3 testSelUpDelF.py
python3 testDataTypes.py
python3 testDataTypesSystem.py
python3 testCTA.py
python3 testSelect.py
python3 testSequence.py
python3 sampleORM.py
python3 sampleORMRelationship.py

exit
#Expected to fail
python3 testInterval.py
python3 testReflection.py
python3 testSelectRand.py
python3 testCreateTableString.py
