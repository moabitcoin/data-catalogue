import json

import avro.ipc
import avro.schema
from avro.io import Validate as validate


def main():
  avro_schema_dic = {
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favorite_number",  "type": ["int", "null"]},
        {"name": "favorite_color", "type": ["string", "null"]}
    ]
  }

  # serialise dict to json string:
  avro_schema_json = json.dumps(avro_schema_dic)

  # parse json string to avro schema
  avro_schema = avro.schema.Parse(json.dumps(avro_schema_dic))

  # python dict conforming to schema:
  message_dict = {'favorite_color': 'black', 'favorite_number': 42, 'name': 'Eli'}

  # json version of table
  message_json = json.dumps(message_dict)

  print("validate message dictionary against schema...")
  if validate(avro_schema,message_dict):
    print("...validation passed")
  else:
    print("...validation failed")

  print("")

  # now pretend we have json strings as input
  message_json_parsed = json.loads(message_json)

  print("validate message string against schema...")
  if validate(avro_schema,message_json_parsed):
    print("...validation passed")
  else:
    print("...validation failed")

main()
