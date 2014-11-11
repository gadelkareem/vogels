'use strict';

var _   = require('lodash'),
    DOC = require('dynamodb-doc');

var serializer = module.exports;

var internals = {};

internals.docClient = new DOC.DynamoDB();

internals.createSet = function(value, type) {
  if(_.isArray(value) ) {
    return internals.docClient.Set(value, type);
  } else {
    return internals.docClient.Set([value], type);
  }
};

var serialize = internals.serialize = {

  binary: function (value) {
    if(_.isString(value)) {
      return internals.docClient.StrToBin(value);
    }

    return value;
  },

  date : function (value) {
    if(_.isDate(value)) {
      return value.toISOString();
    } else {
      return new Date(value).toISOString();
    }
  },

  boolean : function (value) {
    if (value && value !== 'false') {
      return true;
    } else {
      return false;
    }
  },

  stringSet : function (value) {
    return internals.createSet(value, 'S');
  },

  numberSet : function (value) {
    return internals.createSet(value, 'N');
  },

  binarySet : function (value) {
    var bins = value;
    if(!_.isArray(value)) {
      bins = [value];
    }

    var vals = _.map(bins, serialize.binary);
    return internals.createSet(vals, 'B');
  }
};

internals.deserializeAttribute = function (value) {
  if(_.isObject(value) && _.isFunction(value.toArray)) {
    // value is a Set object from dynamodb-doc lib
    return value.toArray();
  } else {
    return value;
  }
};

internals.serializeAttribute = function (value, attr, options) {
  if(!attr || _.isNull(value)) {
    return null;
  }

  options = options || {};

  var type = attr.type._type;

  switch(type){
  case 'boolean':
    return serialize.boolean(value);
  case 'binary':
    return serialize.binary(value);
  case 'date':
    return serialize.date(value);
  case 'numberSet':
    if(options.convertSets) {
      return value;
    }
    return serialize.numberSet(value);
  case 'stringSet':
    if(options.convertSets) {
      return value;
    }
    return serialize.stringSet(value);
  case 'binarySet':
    if(options.convertSets) {
      return serialize.binary(value);
    }
    return serialize.binarySet(value);
  default:
    return value;
  }
};

serializer.buildKey = function (hashKey, rangeKey, schema) {
  var obj = {};

  if(_.isPlainObject(hashKey)) {
    obj[schema.hashKey] = hashKey[schema.hashKey];

    if(schema.rangeKey) {
      obj[schema.rangeKey] = hashKey[schema.rangeKey];
    }

    _.each(schema.globalIndexes, function (keys) {
      if(_.has(hashKey, keys.hashKey)){
        obj[keys.hashKey] = hashKey[keys.hashKey];
      }

      if(_.has(hashKey, keys.rangeKey)){
        obj[keys.rangeKey] = hashKey[keys.rangeKey];
      }
    });

    _.each(schema.secondaryIndexes, function (rangeKey) {
      if(_.has(hashKey, rangeKey)){
        obj[rangeKey] = hashKey[rangeKey];
      }
    });

  } else {
    obj[schema.hashKey] = hashKey;

    if(schema.rangeKey) {
      obj[schema.rangeKey] = rangeKey;
    }
  }

  return serializer.serializeItem(schema, obj);
};

serializer.serializeItem = function (schema, item, options) {
  options = options || {};

  if(!item) {
    return null;
  }

  var serialized = _.reduce(schema.attrs, function (result, attr, key) {
    if(_.has(item, key)) {

      if(options.expected && _.isObject(item[key]) && _.isBoolean(item[key].Exists)) {
        result[key] = item[key];
        return result;
      }

      var val = internals.serializeAttribute(item[key], attr, options);

      if(!_.isNull(val) || options.returnNulls) {
        if(options.expected) {
          result[key] = {'Value' : val};
        } else {
          result[key] = val;
        }
      }
    }

    return result;
  }, {});

  return serialized;
};

serializer.serializeItemForUpdate = function (schema, action, item) {

  return _.reduce(schema.attrs, function (result, attr, key) {
    if(_.has(item, key) && key !== schema.hashKey && key !== schema.rangeKey) {
      var value = item[key];
      if(_.isNull(value)) {
        result[key] = {Action : 'DELETE'};
      } else if (_.isPlainObject(value) && value.$add) {
        result[key] = {Action : 'ADD', Value: internals.serializeAttribute(value.$add, attr)};
      } else if (_.isPlainObject(value) && value.$del) {
        result[key] = {Action : 'DELETE', Value: internals.serializeAttribute(value.$del, attr)};
      } else {
        result[key] = {Action : action, Value: internals.serializeAttribute(value, attr)};
      }
    }

    return result;
  }, {});

};

serializer.deserializeItem = function (item) {

  if(_.isNull(item)) {
    return null;
  }

  var formatter = function (data) {
    var map = _.mapValues;

    if(_.isArray(data)) {
      map = _.map;
    }

    return map(data, function(value) {
      var result;

      if(_.isPlainObject(value)) {
        result = formatter(value);
      } else if(_.isArray(value)) {
        result = formatter(value);
      } else {
        result = internals.deserializeAttribute(value);
      }

      return result;
    });
  };

  return formatter(item);
};
