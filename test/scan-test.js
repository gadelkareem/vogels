'use strict';

var helper = require('./test-helper'),
    Schema = require('../lib/schema'),
    Scan   = require('../lib/scan'),
    _      = require('lodash');

var internals = {};

internals.assertScanFilter = function (scan, expected) {
  var conds = _.map(scan.request.ScanFilter, function (c) {
    return c.format();
  });

  if(!_.isArray(expected)) {
    expected = [expected];
  }

  conds.should.eql(expected);
};

describe('Scan', function () {
  var schema,
      serializer,
      table;

  beforeEach(function () {
    schema = new Schema();
    serializer = helper.mockSerializer(),

    table = helper.mockTable();
    table.tableName = function () {
      return 'accounts';
    };

    table.docClient = helper.mockDocClient();
    table.schema = schema;
  });

  describe('#exec', function () {

    it('should call run scan on table', function (done) {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});

      table.runScan.yields(null, {ConsumedCapacity: {CapacityUnits : 5, TableName: 'accounts'}, Count: 10, ScannedCount: 12});
      serializer.serializeItem.returns({name: {S: 'tim'}});

      new Scan(table, serializer).exec(function (err, results) {
        results.ConsumedCapacity.should.eql({CapacityUnits: 5, TableName: 'accounts'});
        results.Count.should.equal(10);
        results.ScannedCount.should.equal(12);

        done();
      });
    });

    it('should return LastEvaluatedKey', function (done) {
      schema.String('name', {hashKey: true});
      schema.String('email');

      table.runScan.yields(null, {LastEvaluatedKey: {name : 'tim'}, Count: 10, ScannedCount: 12});
      serializer.serializeItem.returns({name: {S: 'tim'}});

      new Scan(table, serializer).exec(function (err, results) {
        results.Count.should.equal(10);
        results.ScannedCount.should.equal(12);

        results.LastEvaluatedKey.should.eql({name : 'tim'});

        done();
      });
    });

  });

  describe('#limit', function () {

    it('should set the limit', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});

      var scan = new Scan(table, serializer).limit(10);

      scan.request.Limit.should.equal(10);
    });

  });

  describe('#scan', function () {

    it('should set array attributes to get', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var scan = new Scan(table, serializer).attributes(['created']);
      scan.request.AttributesToGet.should.eql(['created']);
    });

    it('should set single attribute to get', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var scan = new Scan(table, serializer).attributes('email');
      scan.request.AttributesToGet.should.eql(['email']);
    });

  });

  describe('#startKey', function () {
    it('should set start Key to hash', function () {
      schema.String('name', {hashKey: true});
      schema.String('email');

      var key = {name: {S: 'tim'}};
      serializer.buildKey.returns(key);

      var scan = new Scan(table, serializer).startKey('tim');

      scan.request.ExclusiveStartKey.should.eql(key);
    });

    it('should set start Key to hash + range', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var key = {name: {S: 'tim'}, email : {S: 'foo@example.com'}};
      serializer.buildKey.returns(key);

      var scan = new Scan(table, serializer).startKey({name: 'tim', email: 'foo@example.com'});

      scan.request.ExclusiveStartKey.should.eql(key);
    });
  });

  describe('#select', function () {

    it('should set select Key', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var scan = new Scan(table, serializer).select('COUNT');

      scan.request.Select.should.eql('COUNT');
    });
  });

  describe('#ReturnConsumedCapacity', function () {

    it('should set return consumed capacity Key to passed in value', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var scan = new Scan(table, serializer).returnConsumedCapacity('TOTAL');
      scan.request.ReturnConsumedCapacity.should.eql('TOTAL');
    });

    it('should set return consumed capacity Key', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var scan = new Scan(table, serializer).returnConsumedCapacity();

      scan.request.ReturnConsumedCapacity.should.eql('TOTAL');
    });
  });

  describe('#segment', function () {

    it('should set both segment and total segments keys', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});

      var scan = new Scan(table, serializer).segments(0, 4);

      scan.request.Segment.should.eql(0);
      scan.request.TotalSegments.should.eql(4);
    });
  });


  describe('#where', function () {
    var scan;

    beforeEach(function () {
      scan = new Scan(table, serializer);

      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});
      schema.Date('created', {secondaryIndex: true});
      schema.NumberSet('scores');
    });

    it('should have equals clause', function() {
      scan = scan.where('email').equals('foo@example.com');

      internals.assertScanFilter(scan, {AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'EQ'});
    });

    it('should have not equals clause', function() {
      scan = scan.where('email').ne('foo@example.com');

      internals.assertScanFilter(scan, {AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'NE'});
    });

    it('should have less than or equal clause', function() {
      scan = scan.where('email').lte('foo@example.com');

      internals.assertScanFilter(scan, {AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'LE'});
    });

    it('should have less than clause', function() {
      scan = scan.where('email').lt('foo@example.com');

      internals.assertScanFilter(scan, {AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'LT'});
    });

    it('should have greater than or equal clause', function() {
      scan = scan.where('email').gte('foo@example.com');

      internals.assertScanFilter(scan, {AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'GE'});
    });

    it('should have greater than clause', function() {
      scan = scan.where('email').gt('foo@example.com');

      internals.assertScanFilter(scan, {AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'GT'});
    });

    it('should have not null clause', function() {
      scan = scan.where('email').notNull();

      internals.assertScanFilter(scan, {ComparisonOperator: 'NOT_NULL'});
    });

    it('should have null clause', function() {
      scan = scan.where('email').null();

      internals.assertScanFilter(scan, {ComparisonOperator: 'NULL'});
    });

    it('should have contains clause', function() {
      scan = scan.where('email').contains('foo@example.com');

      internals.assertScanFilter(scan, {AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'CONTAINS'});
    });

    it('should not pass a number set when making contains call', function() {
      scan = scan.where('scores').contains(2);

      internals.assertScanFilter(scan, {AttributeValueList: [{N: '2'}], ComparisonOperator: 'CONTAINS'});
    });

    it('should have not contains clause', function() {
      scan = scan.where('email').notContains('foo@example.com');

      internals.assertScanFilter(scan, {AttributeValueList: [{S: 'foo@example.com'}], ComparisonOperator: 'NOT_CONTAINS'});
    });

    it.skip('should have in clause', function() {
      // TODO there is a bug in dynamodb-doc lib
      // that needs to get fixed til this test can pass
      scan = scan.where('email').in(['foo@example.com', 'test@example.com']);

      var expected ={
        AttributeValueList: [{S: 'foo@example.com'}, {S: 'test@example.com'}],
        ComparisonOperator: 'IN'
      };

      internals.assertScanFilter(scan, expected);
    });

    it('should have begins with clause', function() {
      scan = scan.where('email').beginsWith('foo');

      internals.assertScanFilter(scan, {AttributeValueList: [{S: 'foo'}], ComparisonOperator: 'BEGINS_WITH'});
    });

    it('should have between clause', function() {
      scan = scan.where('email').between('bob@bob.com', 'foo@foo.com');

      var expected = {
        AttributeValueList: [
          {S: 'bob@bob.com'},
          {S: 'foo@foo.com'}
        ],
        ComparisonOperator: 'BETWEEN'
      };

      internals.assertScanFilter(scan, expected);
    });

    it('should have multiple filters', function() {
      scan = scan
        .where('name').equals('Tim')
        .where('email').beginsWith('foo');

      var expected = [
        {AttributeValueList: [{S: 'Tim'}], ComparisonOperator: 'EQ'},
        {AttributeValueList: [{S: 'foo'}], ComparisonOperator: 'BEGINS_WITH'}
      ];

      internals.assertScanFilter(scan, expected);
    });

  });

  describe('#loadAll', function () {

    it('should set load all option to true', function () {
      schema.String('name', {hashKey: true});
      schema.String('email', {rangeKey: true});

      var scan = new Scan(table, serializer).limit(10);

      scan.options.loadAll = true;
    });
  });


});
