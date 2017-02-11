YarnQueueManager
================

This script could do different tasks for helping you to manage your Yarn Queue using the API
Possibilities :
* Read an excel file to generate Yarn Queues configuration
* Request Ambari API to get the actual configuration
* Read the capacity-scheduler.xml to generate the excel file

### TODO
* Read a json file to generate Yarn Queues configuration

### Usage

./YarnQueueManager.py --help
usage: YarnQueueManager [-h] [-v] [-V] [-p] [-d] [-f FROM] [-t TO]
                        [-e XLSFILE] [-j JSONFILE] [-x XMLFILE]

Yarn Queue Manager for setting or reading queues configuration

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         print the version
  -V, --verbose         verbose mode
  -p, --print           print configuration
  -d, --dryRun          Dry run only, nothing is modified
  -f FROM, --from FROM  Get capacity-scheduler configuration from
                        [ambari|xlsFile|xmlFile|jsonFile]
  -t TO, --to TO        Put capacity-scheduler configuration to
                        [ambari|xlsFile|jsonFile]
  -e XLSFILE, --xlsFile XLSFILE
                        Excel file name for get or put
  -j JSONFILE, --jsonFile JSONFILE
                        Json file name for get or put
  -x XMLFILE, --xmlFile XMLFILE
                        Xml file name for get ex : capacity-scheduler.xml


### Examples 

* Get the actual configuration from Ambari API and print the result
    ./YarnQueueManager.py --from ambari -d -p

* Get the excel file configuration and print the result
    ./YarnQueueManager.py --from xlsFile --xlsFile xls/Queues_YARN.xlsm -d -p

* Get the excel file configuration and write it to a json file
    ./YarnQueueManager.py --from xlsFile --xlsFile xls/Queues_YARN.xlsm  -p --to jsonFile --jsonFile json/data.json

* Get the excel file configuration and send it to the Ambari API
    ./YarnQueueManager.py --from xlsFile --xlsFile xls/Queues_YARN.xlsm  -p --to ambari

* Get the actual configuration from the capacity-scheduler.xml file and write it to a json file
    ./YarnQueueManager.py --from xmlFile --xmlFile xml/capacity-scheduler.xml  -p --to jsonFile --jsonFile json/data.json

* Get the actual configuration from the capacity-scheduler.xml file and write it to an excel file
    ./YarnQueueManager.py --from xmlFile --xmlFile xml/capacity-scheduler.xml  -p --to xlsFile --xlsFile xls/Queues_YARN.xlsm

* Get the actual configuration from the capacity-scheduler.xml file and send it to the Ambari API
    ./YarnQueueManager.py --from xmlFile --xmlFile xml/capacity-scheduler.xml  -p --to ambari

### Test config

You could use this command to test the Ambari API of the Hortonworks sandbox
Don't forget to update the tag and the version number

curl -u admin:admin -H "Content-Type: text/plain" -H "X-Requested-By:ambari" -X PUT http://localhost:8080/api/v1/views/CAPACITY-SCHEDULER/versions/1.0.0/instances/AUTO_CS_INSTANCE/resources/scheduler/configuration --data '
{               
  "Clusters": {
    "desired_config": [
      {                                                               
        "service_config_version_note": "Updated by YarnQueueManager", 
        "tag": "version1486849468669", 
        "type": "capacity-scheduler", 
        "version": 7, 
        "properties": {                                       
        "yarn.scheduler.capacity.root.accessible-node-labels": "*",
        "yarn.scheduler.capacity.root.acl_administer_queue": "*",
        "yarn.scheduler.capacity.root.default.acl_administer_jobs": "*",
        "yarn.scheduler.capacity.root.default.capacity": "100",
        "yarn.scheduler.capacity.root.default.user-limit-factor": "1",
        "yarn.scheduler.capacity.root.queues": "default",
        "yarn.scheduler.capacity.root.capacity": "100",
        "yarn.scheduler.capacity.root.default.acl_submit_applications": "*",
        "yarn.scheduler.capacity.root.default.maximum-capacity": "100",
        "yarn.scheduler.capacity.root.default.maximum-am-resource-percent": "0.75",
        "yarn.scheduler.capacity.root.default.state": "RUNNING",
        "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator",
        "yarn.scheduler.capacity.default.minimum-user-limit-percent": "100",
        "yarn.scheduler.capacity.maximum-am-resource-percent": "0.5",
        "yarn.scheduler.capacity.maximum-applications": "10000",        
        "yarn.scheduler.capacity.node-locality-delay": "40"
        }
      }
    ]
  }
}'


