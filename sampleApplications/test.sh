#!bin/bash
#sh test.sh > res 2>&1

if [ "$#" -ne 5 ]; then
    echo "Usage: source test.sh my_host my_user my_password my_db my_port"
    return 1
fi

export MY_HOST=$1
export MY_USER=$2
export MY_PASSWORD=$3
export MY_DB=$4
export MY_PORT=$5

python3 testSelUpDel.py
python3 testSelUpDelF.py
python3 testDataTypes.py
python3 testDataTypesSystem.py
python3 testSelect.py
python3 testSequence.py
python3 sampleORM.py
python3 sampleORMRelationship.py
python3 sampleCore.py
python3 sampleCoreJoins.py
