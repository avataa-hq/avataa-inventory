## Requirements

```
protobuf==3.20.0
confluent-kafka==2.0.2
```

## Add the ability to send messages about new class objects to kafka

### Step 1
Add new message class into protobuf/obj.proto:

- One for unit of class instance 
- Second as list of units


### Step 2
- If you haven’t installed the compiler, download the package and follow the instructions in 
  the README.
- Now run the compiler, specifying the source directory (where your application’s source code lives – the current directory is used if you don’t provide a value), the destination directory (where you want the generated code to go; often the same as $SRC_DIR), and the path to your .proto. In this case, you…:

```
protoc -I=$SRC_DIR --python_out=$DST_DIR $SRC_DIR/addressbook.proto
```

### Step 3
Add new method to your model class    "def to_proto(self)". This method must return an instance 
data in proto format.


### Step 4
In kafka_config/config.py add information about your new class into the dict MODEL_EQ_MESSAGE:
Example:
```
'MO': {'class': MO,
'proto_unit_template': obj_pb2.MO,
'proto_list_template': obj_pb2.ListMO}
```
- 'MO' - class name
- MO - class
- obj_pb2.MO - proto message class for unit of class instance (step 2)
- obj_pb2.ListMO - proto message class for list of class instances (step 2)