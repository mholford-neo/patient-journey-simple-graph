## Demo graph for Simplified Patient Journey

This project builds the graph that is used in the "Let's Build a Stored Procedure in Neo4j - Part I" blog post.

It uses Synthea to build sample patients with Condition events. Then it converts the CSV's created by Synthea into CSV's 
for ingestion by Neo4j admin-import. 

### Building the graph
Once you have cloned the repository, follow these steps to build the graph:
* From the root directory of the project execute:
  ```
  ./gradlew run --args="100000 12345"
  ``` 
* Move to the directory of the CSV's that were created:
    ```
    cd app/output/import
    ```
* Set an environment variable ($ADMIN) with the path to neo4j-admin import
    ```
    export ADMIN=<path to neo4j-admin>
    ```
* Execute the import script
    ```
    ../../src/main/resources/load.sh
    ```
  The script loads into a database called `pj100k` by default. Be sure to run Cypher queries against this database.
  
#### Notes

The main class invoked (SyntheaBuilder.kt) takes two arguments:
1. The number of sample patients to create (REQUIRED)
2. A random number seed (OPTIONAL)

Creating more than 100,000 patients with this utility is not recommended.  
