'use strict';

var serializer = require('../lib/serializer'),
    chai       = require('chai'),
    expect     = chai.expect,
    Schema     = require('../lib/schema'),
    helper     = require('./test-helper');

chai.should();

describe('Serializer', function () {
  var schema;
  var docClient = helper.mockDocClient();

  beforeEach(function () {
    schema = new Schema();
  });

  describe('#buildKeys', function () {

    it('should handle string hash key', function () {
      schema.String('email', {hashKey: true});

      var keys = serializer.buildKey('test@test.com', null, schema);

      keys.should.eql({email: 'test@test.com'});
    });

    it('should handle number hash key', function () {
      schema.Number('year', {hashKey: true});

      var keys = serializer.buildKey(1999, null, schema);

      keys.should.eql({year: 1999});
    });

    it('should handle date hash key', function () {
      schema.Date('timestamp', {hashKey: true});

      var d = new Date();
      var keys = serializer.buildKey(d, null, schema);

      keys.should.eql({timestamp: d.toISOString()});
    });

    it('should handle string hash and range key', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.String('slug');

      var keys = serializer.buildKey('Tim Tester', 'test@test.com', schema);

      keys.should.eql({name: 'Tim Tester', email: 'test@test.com'});
    });

    it('should handle number hash and range key', function () {
      schema.Number('year', {hashKey: true});
      schema.Number('num', {rangeKey: true});

      var keys = serializer.buildKey(1988, 1.4, schema);

      keys.should.eql({year: 1988, num: 1.4});
    });

    it('should handle object containing the hash key', function () {
      schema.Number('year', {hashKey: true});
      schema.String('name', {rangeKey: true});
      schema.String('slug');

      var keys = serializer.buildKey({year: 1988, name : 'Joe'}, null, schema);

      keys.should.eql({year: 1988, name: 'Joe'});
    });

    it('should handle local secondary index keys', function () {
      schema.String('email', {hashKey: true});
      schema.Number('age', {rangeKey: true});
      schema.String('name', { secondaryIndex: true });

      var data = { email : 'test@example.com', age: 22, name: 'Foo Bar' };
      var keys = serializer.buildKey(data, null, schema);

      keys.should.eql({email: 'test@example.com', age: 22, name: 'Foo Bar'});
    });

    it('should handle global secondary index keys', function () {
      schema.String('email', {hashKey: true});
      schema.Number('age');
      schema.String('name');

      schema.globalIndex('GameTitleIndex', {
        hashKey: 'age',
        rangeKey: 'name'
      });

      var data = { email : 'test@example.com', age: 22, name: 'Foo Bar' };
      var keys = serializer.buildKey(data, null, schema);

      keys.should.eql({email: 'test@example.com', age: 22, name: 'Foo Bar'});
    });

    it('should handle boolean global secondary index key', function () {
      schema.String('email', {hashKey: true});
      schema.Number('age');
      schema.String('name');
      schema.Boolean('adult');

      schema.globalIndex('GameTitleIndex', {
        hashKey: 'adult',
        rangeKey: 'email'
      });

      var data = { email : 'test@example.com', adult: false };
      var keys = serializer.buildKey(data, null, schema);

      keys.should.eql({email: 'test@example.com', adult: false});
    });

  });

  describe('#serializeItem', function () {
    it('should serialize string attribute', function () {
      schema.String('name');

      var item = serializer.serializeItem(schema, {name: 'Tim Tester'});

      item.should.eql({name: 'Tim Tester'});
    });

    it('should serialize number attribute', function () {
      schema.Number('age');

      var item = serializer.serializeItem(schema, {age: 21});

      item.should.eql({age: 21});
    });

    it('should serialize binary attribute', function () {
      schema.Binary('data');

      var item = serializer.serializeItem(schema, {data: 'hello'});

      item.should.eql({data: new Buffer('hello')});
    });

    it('should serialize number attribute with value zero', function () {
      schema.Number('age');

      var item = serializer.serializeItem(schema, {age: 0});

      item.should.eql({age: 0});
    });


    it('should serialize boolean attribute', function () {
      schema.Boolean('agree');

      serializer.serializeItem(schema, {agree: true}).should.eql({agree: true});
      serializer.serializeItem(schema, {agree: 'true'}).should.eql({agree: true});

      serializer.serializeItem(schema, {agree: false}).should.eql({agree: false});
      serializer.serializeItem(schema, {agree: 'false'}).should.eql({agree: false});
      //serializer.serializeItem(schema, {agree: null}).should.eql({agree: {N: '0'}});
      serializer.serializeItem(schema, {agree: 0}).should.eql({agree: false});
    });

    it('should serialize date attribute', function () {
      schema.Date('time');

      var d = new Date();
      var item = serializer.serializeItem(schema, {time: d});

      item.should.eql({time: d.toISOString()});
    });

    it('should serialize string set attribute', function () {
      schema.StringSet('names');

      var item = serializer.serializeItem(schema, {names: ['Tim', 'Steve', 'Bob']});

      var stringSet = docClient.Set(['Tim', 'Steve', 'Bob'], 'S');

      item.names.datatype.should.eql('SS');
      item.names.contents.should.eql(stringSet.contents);
    });

    it('should serialize single string set attribute', function () {
      schema.StringSet('names');

      var item = serializer.serializeItem(schema, {names: 'Tim'});

      var stringSet = docClient.Set(['Tim'], 'S');
      item.names.datatype.should.eql('SS');
      item.names.contents.should.eql(stringSet.contents);

    });

    it('should number set attribute', function () {
      schema.NumberSet('scores');

      var item = serializer.serializeItem(schema, {scores: [2, 4, 6, 8]});

      var numberSet = docClient.Set([2, 4, 6, 8], 'N');
      item.scores.datatype.should.eql('NS');
      item.scores.contents.should.eql(numberSet.contents);
    });

    it('should single number set attribute', function () {
      schema.NumberSet('scores');

      var item = serializer.serializeItem(schema, {scores: 2});

      var numberSet = docClient.Set([2], 'N');
      item.scores.datatype.should.eql('NS');
      item.scores.contents.should.eql(numberSet.contents);
    });

    it('should serialize binary set attribute', function () {
      schema.BinarySet('data');

      var item = serializer.serializeItem(schema, {data: ['hello', 'world']});

      var binarySet = docClient.Set([new Buffer('hello'), new Buffer('world')], 'B');
      item.data.datatype.should.eql('BS');
      item.data.contents.should.eql(binarySet.contents);
    });

    it('should serialize single binary set attribute', function () {
      schema.BinarySet('data');

      var item = serializer.serializeItem(schema, {data: 'hello'});

      var binarySet = docClient.Set([new Buffer('hello')], 'B');
      item.data.datatype.should.eql('BS');
      item.data.contents.should.eql(binarySet.contents);
    });

    it('should serialize uuid attribute', function () {
      schema.UUID('id');

      var id = '1234-5123-2342-1234';
      var item = serializer.serializeItem(schema, {id: id});

      item.should.eql({id: id});
    });

    it('should serialize TimeUUId attribute', function () {
      schema.TimeUUID('timeid');

      var timeid = '1234-5123-2342-1234';
      var item = serializer.serializeItem(schema, {timeid: timeid});

      item.should.eql({timeid: timeid});
    });

    it('should return null', function () {
      schema.String('email');
      schema.NumberSet('scores');

      var item = serializer.serializeItem(schema, null);

      expect(item).to.be.null;
    });

    it('should convert string set to a string', function () {
      schema.StringSet('names');

      var item = serializer.serializeItem(schema, {names: 'Bob'}, {convertSets: true});

      item.should.eql({names: 'Bob'});
    });

    it('should serialize string attribute for expected', function () {
      schema.String('name');

      var item = serializer.serializeItem(schema, {name: 'Tim Tester'}, {expected : true});

      item.should.eql({name: { 'Value' : 'Tim Tester'}});
    });

    it('should serialize string attribute for expected exists false', function () {
      schema.String('name');

      var item = serializer.serializeItem(schema, {name: {Exists: false}}, {expected : true});

      item.should.eql({name: { 'Exists' : false}});
    });

  });

  describe('#deserializeItem', function () {
    it('should return string value', function () {
      var itemResp = {name : 'Tim Tester' };

      var item = serializer.deserializeItem(itemResp);

      item.name.should.equal('Tim Tester');
    });

    it('should return values in StringSet', function () {
      var itemResp = {names : docClient.Set(['a', 'b', 'c'], 'S')};

      var item = serializer.deserializeItem(itemResp);

      item.names.should.eql(['a', 'b', 'c']);
    });

    it('should return values in NumberSet', function () {
      var itemResp = {scores : docClient.Set([1, 2, 3], 'N')};

      var item = serializer.deserializeItem(itemResp);

      item.scores.should.eql([1, 2, 3]);
    });

    it('should return null when item is null', function () {
      var item = serializer.deserializeItem(null);

      expect(item).to.be.null;
    });

    it('should return nested values', function () {
      var itemResp = {
        name : 'foo bar',
        scores : docClient.Set([1, 2, 3], 'N'),
        things : [{
          title : 'item 1',
          letters : docClient.Set(['a', 'b', 'c'], 'S')
        }, {
          title : 'item 2',
          letters : docClient.Set(['x', 'y', 'z'], 'S')
        }],
        info : {
          name : 'baz',
          ages : docClient.Set([20, 21, 22], 'N')
        }
      };

      var item = serializer.deserializeItem(itemResp);

      item.should.eql({
        name : 'foo bar',
        scores : [1, 2, 3],
        things : [{
          title : 'item 1',
          letters : ['a', 'b', 'c']
        }, {
          title : 'item 2',
          letters : ['x', 'y', 'z']
        }],
        info : {
          name : 'baz',
          ages : [20, 21, 22]
        }
      });
    });

  });

  describe('#serializeItemForUpdate', function () {
    it('should serialize string attribute', function () {
      schema.String('name');

      var item = serializer.serializeItemForUpdate(schema, 'PUT', {name: 'Tim Tester'});

      item.should.eql({ name: {Action: 'PUT', Value: 'Tim Tester'}});
    });

    it('should serialize number attribute', function () {
      schema.Number('age');

      var item = serializer.serializeItemForUpdate(schema, 'PUT', {age: 25});

      item.should.eql({ age: {Action: 'PUT', Value: 25}});
    });

    it('should serialize three attributes', function () {
      schema.String('name');
      schema.Number('age');
      schema.NumberSet('scores');

      var attr = {name: 'Tim Test', age: 25, scores: [94, 92, 100]};
      var item = serializer.serializeItemForUpdate(schema, 'PUT', attr);

      item.name.should.eql({Action : 'PUT', Value : 'Tim Test'});
      item.age.should.eql({Action : 'PUT', Value : 25});

      var numberSet = docClient.Set([94, 92, 100], 'N');
      item.scores.Action.should.eql('PUT');
      item.scores.Value.datatype.should.eql('NS');
      item.scores.Value.contents.should.eql(numberSet.contents);
    });

    it('should serialize null value to a DELETE action', function () {
      schema.String('name');
      schema.Number('age');

      var item = serializer.serializeItemForUpdate(schema, 'PUT', {age: null, name : 'Foo Bar'});

      item.should.eql({
        name: {Action: 'PUT', Value: 'Foo Bar' },
        age:  {Action: 'DELETE'}
      });
    });

    it('should not serialize hashkey attribute', function () {
      schema.String('email', {hashKey: true});
      schema.String('name');

      var item = serializer.serializeItemForUpdate(schema, 'PUT', {email: 'test@test.com', name: 'Tim Tester'});

      item.should.eql({ name: {Action: 'PUT', Value: 'Tim Tester' }});
    });

    it('should not serialize hashkey and rangeKey attributes', function () {
      schema.String('email', {hashKey: true});
      schema.String('range', {rangeKey: true});
      schema.String('name');

      var item = serializer.serializeItemForUpdate(schema, 'PUT', {email: 'test@test.com', range: 'FOO', name: 'Tim Tester'});

      item.should.eql({ name: {Action: 'PUT', Value: 'Tim Tester'}});
    });

    it('should serialize add operations', function () {
      schema.String('email', {hashKey: true});
      schema.Number('age');
      schema.StringSet('names');

      var update = {email: 'test@test.com', age: {$add : 1}, names : {$add: ['foo', 'bar']}};
      var item = serializer.serializeItemForUpdate(schema, 'PUT', update);

      item.age.should.eql({Action: 'ADD', Value: 1});

      var stringSet = docClient.Set(['foo', 'bar'], 'S');
      item.names.Action.should.eql('ADD');
      item.names.Value.datatype.should.eql('SS');
      item.names.Value.contents.should.eql(stringSet.contents);
    });

    it('should serialize delete operations', function () {
      schema.String('email', {hashKey: true});
      schema.StringSet('names');
      schema.NumberSet('ages');

      var update = {email: 'test@test.com', ages: {$del : [2, 3]}, names : {$del: ['foo', 'bar']}};
      var item = serializer.serializeItemForUpdate(schema, 'PUT', update);

      var stringSet = docClient.Set(['foo', 'bar'], 'S');
      item.names.Action.should.eql('DELETE');
      item.names.Value.datatype.should.eql('SS');
      item.names.Value.contents.should.eql(stringSet.contents);

      var numberSet = docClient.Set([2, 3], 'N');
      item.ages.Action.should.eql('DELETE');
      item.ages.Value.datatype.should.eql('NS');
      item.ages.Value.contents.should.eql(numberSet.contents);

    });

  });
});
