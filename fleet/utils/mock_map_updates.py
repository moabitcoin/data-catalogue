import uuid


def build_mock_collection(diary_token):

  collection = dict({})
  collection['diary_token'] = diary_token
  collection['drive_tokens'] = []
  collection['feature_tokens'] = []
  collection['geo_json'] = {"type": "FeatureCollection",
                            "features": [{"coordinates": [],
                                          'type': "MultiPoint"}]}

  return collection


def add_feature_to_collection(collection, location):

  collection['geo_json']["features"][0]["coordinates"].append(location)


def build_mock_feature(diary_token, drive_token, sequence_token,
                       element_token, data_token, **kwargs):

  feature = dict({})
  feature['diary_token'] = diary_token
  feature['drive_token'] = drive_token
  feature['sequence_token'] = sequence_token
  feature['element_token'] = element_token
  feature['data_token'] = data_token
  feature['feature_token'] = uuid.uuid4().hex

  for key, val in kwargs.items():
    feature[key] = val

  return feature
