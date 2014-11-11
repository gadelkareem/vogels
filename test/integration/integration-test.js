'use strict';

var vogels = require('../../index'),
    chai   = require('chai'),
    expect = chai.expect,
    async  = require('async'),
    _      = require('lodash'),
    helper = require('../test-helper');

chai.should();

vogels.dynamoDriver(helper.realDynamoDB());

var User = vogels.define('vogels-int-test-user', function (schema) {
  schema.UUID('id', {hashKey: true});
  schema.String('email').required();
  schema.String('name');
  schema.Number('age').min(10);
  schema.StringSet('roles', {default : ['user']});
  schema.Boolean('acceptedTerms', {default : false});
});

var Tweet = vogels.define('vogels-int-test-tweet', function (schema) {
  schema.String('UserId', {hashKey: true});
  schema.UUID('TweetID', {rangeKey: true});
  schema.String('content');
  schema.Number('num');
  schema.String('tag');

  schema.Date('PublishedDateTime', {secondaryIndex: true, default : Date.now});
});

var internals = {};

internals.userId = function (n) {
  return 'userid-' + n;
};

internals.loadSeedData = function (callback) {
  callback = callback || _.noop;

  async.parallel([
    function (callback) {
      async.times(15, function(n, next) {
        var roles = ['user'];
        if(n % 3 === 0) {
          roles = ['admin', 'editor'];
        } else if (n %5 === 0) {
          roles = ['qa', 'dev'];
        }

        User.create({id : internals.userId(n), email: 'test' + n + '@example.com', name : 'Test ' + n %3, age: n +10, roles : roles}, next);
      }, callback);
    },
    function (callback) {
      async.times(15 * 5, function(n, next) {
        var userId = internals.userId( n %5);
        var p = {UserId : userId, content: 'I love tweeting, in fact Ive tweeted ' + n + ' times', num : n};
        if(n %3 === 0 ) {
          p.tag = '#test';
        }

        Tweet.create(p, next);
      }, callback);
    }
  ], callback);
};

describe('Vogels Integration Tests', function() {
  this.timeout(0);

  before(function (done) {
    async.series([
      async.apply(vogels.createTables.bind(vogels)),
      function (callback) {
        User.create({id : '123456789', email : 'some@user.com', age: 30}, callback);
      },
      internals.loadSeedData
    ], done);
  });

  describe('#create', function () {
    it('should create item with hash key', function(done) {
      User.create({
        email : 'foo@bar.com',
        age : 18,
        roles : ['user', 'admin'],
        acceptedTerms : true
      }, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms']);
        return done();
      });
    });
  });

  describe('#get', function () {
    it('should get item by hash key', function(done) {
      User.get({ id : '123456789'}, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms']);
        return done();
      });
    });

    it('should get return only selected attributes', function(done) {
      User.get({ id : '123456789'},{AttributesToGet : ['email', 'age']}, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['email', 'age']);
        return done();
      });
    });

  });

  describe('#update', function () {
    it('should update item appended role', function(done) {
      User.update({
        id : '123456789',
        roles  : {$add : 'tester'}
      }, function (err, acc) {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms']);
        expect(acc.get('roles')).to.eql(['user', 'tester']);
        return done();
      });
    });
  });

  describe('#getItems', function () {
    it('should return 3 items', function(done) {
      User.getItems(['userid-1', 'userid-2', 'userid-3'], function (err, accounts) {
        expect(err).to.not.exist;
        expect(accounts).to.have.length(3);
        return done();
      });
    });

    it('should return 2 items with only selected attributes', function(done) {
      var opts = {AttributesToGet : ['email', 'age']};

      User.getItems(['userid-1', 'userid-2'], opts, function (err, accounts) {
        expect(err).to.not.exist;
        expect(accounts).to.have.length(2);
        _.each(accounts, function (acc) {
          expect(acc.get()).to.have.keys(['email', 'age']);
        });

        return done();
      });
    });
  });

  describe('#query', function () {
    it('should return users tweets', function(done) {
      Tweet.query('userid-1').exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (t) {
          expect(t.get('UserId')).to.eql('userid-1');
        });

        return done();
      });
    });

    it('should return tweets using secondaryIndex', function(done) {
      Tweet.query('userid-1')
      .usingIndex('PublishedDateTimeIndex')
      .descending()
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        var prev;
        _.each(data.Items, function (t) {
          expect(t.get('UserId')).to.eql('userid-1');

          var published = t.get('PublishedDateTime');

          if(prev) {
            expect(published).to.be.below(prev);
          }

          prev = published;
        });

        return done();
      });
    });

    it('should return tweets using secondaryIndex and date object', function(done) {
      var twoSecAgo = new Date(new Date().getTime() - 2*1000);

      Tweet.query('userid-1')
      .usingIndex('PublishedDateTimeIndex')
      .where('PublishedDateTime').gt(twoSecAgo)
      .descending()
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        var prev;
        _.each(data.Items, function (t) {
          expect(t.get('UserId')).to.eql('userid-1');

          var published = t.get('PublishedDateTime');

          if(prev) {
            expect(published).to.be.below(prev);
          }

          prev = published;
        });

        return done();
      });
    });

    it('should return tweets that match filters', function(done) {
      Tweet.query('userid-1')
      .filter('num').between(4, 8)
      .filter('tag').exists()
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (t) {
          expect(t.get('UserId')).to.eql('userid-1');
          expect(t.get('num')).to.be.above(3);
          expect(t.get('num')).to.be.below(9);
          expect(t.get('tag')).to.exist();
        });

        return done();
      });
    });
  });


  describe('#scan', function () {
    it('should return all users', function(done) {
      User.scan().loadAll().exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        return done();
      });

    });

    it('should return 10 users', function(done) {
      User.scan().limit(10).exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length(10);

        return done();
      });
    });

    it('should return users older than 18', function(done) {
      User.scan()
      .where('age').gt(18)
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (u) {
          expect(u.get('age')).to.be.above(18);
        });

        return done();
      });
    });

    it('should return users matching multiple filters', function(done) {
      User.scan()
      .where('age').between(18, 22)
      .where('email').beginsWith('test1')
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (u) {
          expect(u.get('age')).to.be.within(18, 22);
          expect(u.get('email')).to.match(/^test1.*/);
        });

        return done();
      });
    });

    it('should return users contains admin role', function(done) {
      User.scan()
      .where('roles').contains('admin')
      .exec(function (err, data) {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, function (u) {
          expect(u.get('roles')).to.include('admin');
        });

        return done();
      });
    });
  });
});
