# Pyspark Streaming 

This repo consists in a machine learning streaming pipeline. 
The project uses Pyspark Streaming for:
    
* Reading messages from a Kafka topic.
* Processing batch and using a machine learning model to make predictions.
* Delivering batch of predictions to an output Kafka topic.

### Building Docker image

Run `docker compose build ` in order to create a Docker image of the app.

### Try the app

* You can use the example model package `lr-sent-analyses`, located in `test/models/`. 
This is a NLP model that predicts the 'mood' related to a certain comment, for example `'I love this' --> 'good'`. 
Make a copy of the folder and put it inside the `models/` docker volume.

* Open docker-compose.yml and check that the environment variable `PYSPARK_MODEL_PIPELINE` is defined as `lr-sent-analyses`, so the app can consume the model pipeline. 

* Run `bash init.sh`. This scrip will raise the docker containers and create three kafka topics:
 `messages`, `predictions` and `errors`. In the first topic there will be the incoming messages, whereas in 
the second topic predictions will be sent. The third one is reserved for invalid messages.
(Wait until the app is ready, by checking the docker container logs.)

* Then you may run the Python script `example.py` inside the `playground` folder. It creates a Kafka producer and consumer, 
sends a few messages to the first topic and finally waits for the predictions listening the 'predictions' topic.

* Otherwise, you can create a Producer and Consumer by hand:


1. Open a console and create a Kafka Consumer that listens to 'predictions' topic.
    ```    
    docker compose exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic predictions
    ```
2. Create your own Producer in a new console by 
    ```
    docker compose exec broker kafka-console-producer --bootstrap-server localhost:9092 --topic messages
    ```  
   Then, send messages with the following schema
    ```
        {
          "id": <int>, 
          "clean_text": <str>
        }
    ```

3. To consume the errors topic:
    ```  
    docker compose exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic errors
    ```


### Pipeline Model package
In this project, a model package consists in a PipelineModel trained with Pyspark and exported to disk.
The files will be organized in a directory, named as the model itself, with the following files/folders:

```
   <pipeline_model_name>/
              |___ metadata/: folder with metadata files (created when exporting the pipeline model with Pyspark.)
              |___ stages/: folder with files that encodes the stages of the pipeline. (created when exporting the pipeline model with Pyspark.)
              |___ config.json: A configuration file necessary for consuming the model with this app.
```

The `config.json` must have information related to the pipeline model input and output:
```
        {
          "input_schema": {
            <input_model_col>: "STRING" | "INTEGER" | "DOUBLE"
            ...
          },
          "output_labels": {
              <output model label>: <target label>
          }
        }
```
For ex:
```
        {
          "input_schema": {
            "feature_A": "STRING",
            "feature_B": "DOUBLE",
            "feature_C": "DOUBLE"
          },
          "output_labels": {
              "0": "label1",
              "1": "label2",
              "2": "label3"
          }
        }
```
In this case, the PipelineModel needs an input DataFrame with the columns named "feature_A", "feature_B" and 
"feature_C", with data types String, Double and Double (this is defined in "input_schema".) The Pipeline output will be an integer number: 
0, 1 or 2; so, they will be transformed to strings according the rules defined in `"output_labels"`. If the Pipeline's outputs don't need 
a transformation, just leave `"output_labels"` as an empty object `{}`

The model package must be placed inside the `models/` docker volume.
   
_________________________________

#### TODO: Add jupyter notebook for training a pipeline model.