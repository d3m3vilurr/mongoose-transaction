/* eslint-env node, mocha */
'use strict';
const Promise = require('songbird');
const should = require('should');
const mongoose = require('mongoose');
const _ = require('lodash');
global.TRANSACTION_DEBUG_LOG = false;
const transaction = require('../src/index');
const DEFINE = require('../src/define');
const utils = require('../src/utils');
const ERRORS = DEFINE.ERROR_TYPE;

let connection;
let Test;
let Transaction;

const ma = (fn) => {
    return (done) => {
        fn.call().then(done).catch(done);
    };
};

const initialize = (callback) => {
    let config;
    try {
        config = require('./config');
    } catch (e) {
        config = {mongodb: 'localhost:27017'};
    }
    let dbname = 'test_transaction_' + (+new Date());
    let uri = 'mongodb://' + config.mongodb + '/' + dbname;
    console.log(uri);
    connection = mongoose.createConnection(uri, callback);
};

const TestSchema = new mongoose.Schema({
    num: {type: Number, max: 5},
    string: String,
    def: {type: Number, required: true, default: 1},
}, {shardKey: {_id: 1}});

const getNative = async function() {
    return await this.collection.promise.findOne({_id: this._id});
};

TestSchema.methods.getNative = getNative;
transaction.TransactionSchema.methods.getNative = getNative;
before(ma(async() => {
    await initialize.promise();

    Test = transaction.TransactedModel(connection, 'Test', TestSchema);
    // FIXME: need init process
    transaction.TransactionSchema.plugin(
        transaction.bindShardKeyRule,
        {
            fields: {shard: {type: Number, required: true}},
            rule: {shard: 1, _id: 1},
            initialize: (doc) => {
                doc.shard = doc.shard || doc._id.getTimestamp().getTime();
            },
        }
    );
    Transaction = connection.model(transaction.TRANSACTION_COLLECTION,
                                   transaction.TransactionSchema);
    transaction.addCollectionPseudoModelPair(
        Transaction.collection.name, connection,
        transaction.TransactionSchema
    );
}));

beforeEach(function() {
    this.timeout(10000);
});

afterEach((done) => {
    if (!connection || !connection.db) {
        return done();
    }
    connection.db.dropDatabase(done);
});

const createSavedTestDoc = async(obj = null) => {
    if (obj === null) {
        obj = {num: 1};
    }
    const d = new Test(obj);
    await d.promise.save();
    return d;
}

describe('TransactedModel', function() {
    it('should have transaction lock at create new doucment', ma(async() => {
        const x = await createSavedTestDoc();
        x.t.should.eql(transaction.NULL_OBJECTID);
    }));

    it('should have transaction lock at fetch document from database',
        ma(async() => {
            const x = await createSavedTestDoc();
            const doc = await Test.promise.findById(x._id);
            doc.t.should.eql(transaction.NULL_OBJECTID);
        }));

    it('should fetch lock and sharding fields if not exists at fetch targets',
        ma(async() => {
            const x = await createSavedTestDoc();
            const test = await Test.promise.findById(x._id, 'num');
            should.exists(test.t);
            should.exists(test._id);
        }));

    it('result of toJSON should remove lock field', ma(async() => {
        const x = await createSavedTestDoc();
        const doc = await Test.promise.findById(x._id);
        doc.toJSON().should.not.have.property('t');
    }));

    it('can be try fetch non exist document', ma(async() => {
        const id = new mongoose.Types.ObjectId();
        const doc = await Test.promise.findOne({_id: id});
        should.not.exists(doc);
    }));
});

describe('Save with transaction', function() {
    it('transaction add should check validate schema', ma(async() => {
        const x = await createSavedTestDoc();
        const t = await Transaction.begin();
        x.num = 10;
        try {
            await t.add(x);
            should.fail('no error was thrown');
        } catch (e) {
            e.name.should.eql('ValidationError');
        }
    }));

    it('update can be possible', ma(async() => {
        const x = await createSavedTestDoc();
        const t = await Transaction.begin();
        await t.add(x);

        x.num = 2;

        let nx = await x.getNative();
        t._id.should.eql(nx.t);
        nx.num.should.eql(1);
        nx.def.should.eql(1);
        await t.commit();

        nx = await x.getNative();
        nx.t.should.eql(transaction.NULL_OBJECTID);
        nx.num.should.eql(2);
        should.not.exists(await t.getNative());
    }));

    it('can make new document', ma(async() => {
        const t = await Transaction.begin();
        const x = new Test({num: 1});
        await t.add(x);

        let nx = await x.getNative();
        t._id.should.eql(nx.t);
        should.not.exist(nx.num);
        nx.__new.should.eql(true);
        await t.commit();

        nx = await x.getNative();
        nx.t.should.eql(transaction.NULL_OBJECTID);
        nx.num.should.eql(1);
        nx.def.should.eql(1);
        should.not.exists(nx.__new);
        should.not.exists(await t.getNative());
    }));

    it('if cancel transaction process and contains new documents, ' +
            'should cancel make new documents',
        ma(async() => {
            const t = await Transaction.begin();
            const x = new Test({num: 1});
            await t.add(x);
            try {
                await t.cancel('testcase');
            } catch (e) {}
            should.not.exists(await x.getNative());
            should.not.exists(await t.getNative());
        }));

    it('if stop in the middle of transaction process,' +
            'should cancel make new documents',
        ma(async() => {
            const t = await Transaction.begin();
            const x = new Test({num: 1});
            await t.add(x);
            await t.remove();
            const xx = await Test.promise.find({_id: x._id});
            should.exists(xx);
            xx.length.should.eql(0);
        }));

    it('should support multiple documents with transaction', ma(async() => {
        const x = await createSavedTestDoc();
        const t = await Transaction.begin();
        await t.add(x);

        x.num = 2;
        const y = new Test({string: 'abcd'});
        await t.add(y);
        await t.commit();
        (await x.getNative()).num.should.eql(2);
        (await y.getNative()).string.should.eql('abcd');
        should.not.exists(await t.getNative());
    }));

    it('should support remove document with transaction', ma(async() => {
        const x = await createSavedTestDoc();
        const t = await Transaction.begin();

        await t.removeDoc(x);
        await t.commit();
        should.not.exists(await x.getNative());
    }));

    it('if cancel transaction process, also cancel reserved remove document',
        ma(async() => {
            const x = await createSavedTestDoc();
            const t = await Transaction.begin();

            await t.removeDoc(x);
            await t.expire();
            should.exists(await x.getNative());
        }));
});

describe('Find documents from model', function() {
    it('auto commit before load data', ma(async() => {
        const x = await createSavedTestDoc();
        const t = await Transaction.begin();
        await t.add(x);

        x.num = 2;
        await t._commit();
        const xx = await Test.promise.findById(x.id);
        xx.t.should.eql(transaction.NULL_OBJECTID);
        xx.num.should.eql(2);
    }));

    it('find fetch all documents of matched, ' +
            'they should finish commit process of previous transaction',
        ma(async() => {
            const x = await createSavedTestDoc();
            const y = await createSavedTestDoc();
            const t = await Transaction.begin();

            x.num = 2;
            y.num = 2;
            await t.add(x);
            await t.add(y);
            await t._commit();

            const docs = await Test.promise.find({});
            should.exists(docs);
            docs.length.should.eql(2);
            docs.forEach((d) => {
                d.t.should.eql(transaction.NULL_OBJECTID);
                d.num.should.eql(2);
            });
        }));

    it('findById fetch a document, ' +
            'it should cancel removed previous transaction',
        ma(async() => {
            const x = await createSavedTestDoc();
            await Test.collection.promise.update(
                    {_id: x._id},
                    {$set: {t: new mongoose.Types.ObjectId()}}
            );
            const xx = await Test.promise.findById(x._id);
            should.exists(xx);
            xx.t.should.eql(transaction.NULL_OBJECTID);
        }));

    it('find fetch all documents of matched, ' +
            'they should cancel removed previous transaction',
        ma(async() => {
            const x = await createSavedTestDoc();
            await Test.collection.promise.update(
                    {_id: x._id},
                    {$set: {t: new mongoose.Types.ObjectId()}}
            );
            await createSavedTestDoc({
                num: 2, t: new mongoose.Types.ObjectId(),
            });
            const docs = await Test.promise.find({});
            should.exists(docs);
            docs.length.should.eql(2);
            docs.forEach((x) => x.t.should.eql(transaction.NULL_OBJECTID));
        }));

    it('findOne should wait previous transaction lock', ma(async() => {
        const x = await createSavedTestDoc();
        const t = await Transaction.begin();
        await t.add(x);

        // var st = +new Date();
        try {
            await Test.promise.findOne({_id: x._id});
            should.fail('no error was thrown');
        } catch (e) {
            e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
        }
        // ((+new Date()) - st >= 37 * 5).should.be.true;
    }));

    it('findOneNatvie fetch a native mongo document of matched, ' +
            'it should cancel removed previous transaction',
        ma(async() => {
            const x = await createSavedTestDoc({
                num: 1, t: new mongoose.Types.ObjectId(),
            });

            const nx = await Test.promise.findOneNative({_id: x._id});
            should.exists(nx);
            nx.t.should.eql(transaction.NULL_OBJECTID);
        }));

    it('findNatvie fetch all native mongo documents of matched, ' +
            'they should cancel removed previous transaction',
        ma(async() => {
            await createSavedTestDoc({
                num: 1, t: new mongoose.Types.ObjectId(),
            });
            await createSavedTestDoc({
                num: 1, t: new mongoose.Types.ObjectId(),
            });

            const ndocs = await Test.promise.findNative({});
            should.exists(ndocs);
            const count = await ndocs.promise.count();
            should.exists(count);
            count.should.not.eql(0);
            ndocs.rewind();
            const docs = await ndocs.promise.toArray();
            should.exists(docs);
            docs.length.should.eql(count);
            docs.forEach((x) => x.t.should.eql(transaction.NULL_OBJECTID));
        }));

    it('can be force fetch document(ignore transaction lock)', ma(async() => {
        const x = await createSavedTestDoc();
        const t = await Transaction.begin();
        await t.add(x);

        const nx = await Test.promise.findOneForce({_id: x._id});
        should.exists(nx);
        nx.t.should.eql(t._id);
    }));
});

describe('Find documents from transaction', function() {
    it('findOne fetch a document and automatic set transaction lock',
        ma(async() => {
            const x = await createSavedTestDoc();
            const t = await Transaction.begin();

            const xx = await t.findOne(Test, {_id: x._id});
            should.exist(xx);
            xx.t.should.not.eql(transaction.NULL_OBJECTID);
            should.exist(xx.t);
            xx.num = 2;
            await t.commit();
            (await x.getNative()).num.should.eql(2);
        }));

    it('findOne fetch a document of matched, ' +
            'it should finish commit process of previous transaction',
        ma(async() => {
            const x = await createSavedTestDoc();
            const t0 = await Transaction.begin();
            await t0.add(x);
            x.num = 2;
            await t0._commit();

            const t1 = await Transaction.begin();
            // FIXME: make TransactionSchema.findById
            // t2.findById(Test, x.id, sync.defer());
            const xx = await t1.findOne(Test, {_id: x._id});
            should.exists(xx);
            xx.t.should.eql(t1._id);
            xx.num.should.eql(2);
        }));

    it('find fetch documents & automatic set transaction lock', ma(async() => {
        const x = await createSavedTestDoc();
        const t = await Transaction.begin();

        const docs = await t.find(Test, {_id: x._id});
        should.exist(docs);
        Array.isArray(docs).should.be.true;
        docs.length.should.be.eql(1);
        const xx = docs[0];
        xx.t.should.not.eql(transaction.NULL_OBJECTID);
        should.exist(xx.t);
        xx.num = 2;
        await t.commit();
        (await x.getNative()).num.should.eql(2);
    }));

    it('Transaction.findOne should support sort option', ma(async() => {
        const t = await Transaction.begin();
        await (new Test()).promise.save();
        await (new Test()).promise.save();
        const t0 = await t.findOne(Test, null, {sort: {'_id': 1}});
        should.exist(t0);
        const t1 = await t.findOne(Test, null, {sort: {'_id': -1}});
        should.exist(t1);
        t0._id.should.not.eql(t1._id);
    }));

    // FIXME: current find only check t is `NULL_OBJECTID`
    // so, docs.length is always return 0
    xit('find fetch all documents of matched, ' +
            'they should finish commit process of previous transaction',
        ma(async() => {
            const t0 = await Transaction.begin();
            const x = await createSavedTestDoc();
            const y = await createSavedTestDoc();

            x.num = 2;
            y.num = 2;
            await t0.add(x);
            await t0.add(y);
            await t0._commit();

            const t1 = await Transaction.begin();
            const docs = await t1.find(Test, {});
            should.exists(docs);
            docs.length.should.eql(2);
            docs.forEach((d) => {
                d.t.should.eql(transaction.NULL_OBJECTID);
                d.num.should.eql(2);
            });
        }));

    it('findOne fetch a document of matched, ' +
            'it should finish commit process of previous transaction',
        ma(async() => {
            const t = await Transaction.begin();
            const x = await createSavedTestDoc({
                num: 1, t: new mongoose.Types.ObjectId(),
            });
            // t.findById(Test, x._id, sync.defer());
            const xx = await t.findOne(Test, {_id: x._id});
            should.exists(xx);
            xx.t.should.eql(t._id);
        }));

    // FIXME: current find only check t is `NULL_OBJECTID`
    // so, docs.length is always return 0
    xit('find fetch all documents of matched, ' +
            'they should cancel removed previous transaction',
        ma(async() => {
            const t = await Transaction.begin();
            await createSavedTestDoc({
                num: 1, t: new mongoose.Types.ObjectId(),
            });
            await createSavedTestDoc({
                num: 2, t: new mongoose.Types.ObjectId(),
            });
            const docs = await t.find(Test, {});
            docs.length.should.eql(2);
            docs.forEach((x) => x.t.should.eql(transaction.NULL_OBJECTID));
        }));

    it('find fetch documents and automatic set transaction lock',
        ma(async() => {
            const x = await createSavedTestDoc();
            const t = await Transaction.begin();

            const docs = await t.find(Test, {_id: x._id});
            should.exist(docs);
            Array.isArray(docs).should.be.true;
            docs.length.should.be.eql(1);
            const xx = docs[0];
            xx.t.should.not.eql(transaction.NULL_OBJECTID);
            should.exist(xx.t);
            xx.num = 2;
            await t.commit();
            (await x.getNative()).num.should.eql(2);
        }));
});

describe('Transaction conflict', function() {
    it('above two transaction mark manage document mark at the same time',
        ma(async() => {
            const t0 = await Transaction.begin();
            const x = await createSavedTestDoc();

            const t1 = await Transaction.begin();
            const xx = await Test.promise.findById(x.id);
            x.num = 2;
            await t0.add(x);
            xx.num = 3;
            try {
                await t1.add(xx);
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_1);
            }
        }));

    it('already transacted document try save on another process',
        ma(async() => {
            const x = await createSavedTestDoc();
            const t = await Transaction.begin();

            const xx = await Test.promise.findById(x.id);
            x.num = 2;
            await t.add(x);
            xx.num = 3;

            try {
                await xx.promise.save();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_1);
            }
            await t.commit();

            const nx = await x.getNative();
            nx.t.should.eql(transaction.NULL_OBJECTID);
            nx.num.should.eql(2);
        }));

    it('(normal)not transacted document try save on another process',
        ma(async() => {
            const x = await createSavedTestDoc();
            const xx = await Test.promise.findOne({_id: x._id});
            xx.num = 2;
            await xx.promise.save();

            const nx = await x.getNative();
            nx.t.should.eql(transaction.NULL_OBJECTID);
            nx.num.should.eql(2);
        }));

    it.skip('(broken) we cannot care manually sequential update ' +
                'as fetched document without transaction',
        ma(async() => {
            const x = await createSavedTestDoc();

            const x0 = await Test.promise.findOne({_id: x._id});
            const x1 = await Test.promise.findOne({_id: x._id});
            const t0 = await Transaction.begin();

            x0.num = 2;
            await t0.add(x0);
            await t0.commit();

            const t1 = await Transaction.begin();
            x1.num = 3;
            await t1.add(x1);
            should.fail('no error was thrown');
            await t1.commit();
        }));

    it('findOne from transaction prevent race condition when fetch a document',
        ma(async() => {
            const x = await createSavedTestDoc();
            const t = await Transaction.begin();

            const xx = await t.findOne(Test, {_id: x._id});
            should.exist(xx);
            xx.t.should.not.eql(transaction.NULL_OBJECTID);
            try {
                await t.findOne(Test, {_id: x._id});
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
        }));
});

describe('Transaction lock', function() {
    it('model.findById should raise error at try fetch to locked document',
        ma(async() => {
            const x = await createSavedTestDoc();
            const t = await Transaction.begin();

            x.num = 2;
            await t.add(x);
            try {
                await Test.promise.findById(x.id);
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
        }));

    it('model.findOne should wait unlock previous transaction lock',
        ma(async() => {
            const x = await createSavedTestDoc();
            const t = await Transaction.begin();
            await t.add(x);

            const promise0 = (async() => {
                const xx = await Test.promise.findOne({_id: x._id});
                should.exists(xx);
                xx.t.should.eql(transaction.NULL_OBJECTID);
            })();

            const promise1 = (async() => {
                await utils.sleep(100);
                await t.commit();
            })();
            await Promise.all([promise0, promise1]);
        }));

    it('transaction.findOne should raise error ' +
            'at try fetch to locked document ' +
            'and previous transaction was alive',
        ma(async() => {
            const x = await createSavedTestDoc();
            const t0 = await Transaction.begin();
            await t0.add(x);

            const t1 = await Transaction.begin();
            // const st = +new Date();
            try {
                await t1.findOne(Test, {_id: x._id});
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
            // ((+new Date()) - st >= 37 * 5).should.be.true;
        }));

    it('transaction.findOne should wait unlock previous transaction lock',
        ma(async() => {
            const x = await createSavedTestDoc();
            const t0 = await Transaction.begin();
            await t0.add(x);

            const t1 = await Transaction.begin();

            const promise0 = (async() => {
                const xx = await t1.findOne(Test, {_id: x._id});
                should.exists(xx);
                xx.t.should.eql(t1._id);
            })();
            const promise1 = (async() => {
                await utils.sleep(100);
                await t0.commit();
            })();
            await Promise.all([promise0, promise1]);
        }));

    it('overtime transaction should expire automatically', ma(async() => {
        const beforeGap =
            +new Date() - transaction.TRANSACTION_EXPIRE_GAP;
        const t = new Transaction({
            _id: mongoose.Types.ObjectId.createFromTime(beforeGap / 1000),
        });
        // wrapTransactionMethods(t);
        const x = new Test({num: 1});
        await t.begin();
        await t.add(x);
        try {
            await t.commit();
            should.fail('no error was thrown');
        } catch (e) {
            e.message.should.eql(ERRORS.TRANSACTION_EXPIRED);
        }
        should.not.exists(await x.getNative());
    }));
});

describe('Transaction state conflict', function() {
    it('already committed transaction cannot move expire state',
        ma(async() => {
            const t = await Transaction.begin();
            await t._commit();
            await t.expire();
            t.state.should.eql('commit');
        }));

    it('already expired transaction cannot move commit state',
        ma(async() => {
            const t = await Transaction.begin();
            await t._expire();
            try {
                await t.commit();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_EXPIRED);
            }
            t.state.should.eql('expire');
        }));

    it('if transaction expired for another process, cannot move commit state',
        ma(async() => {
            const t0 = await Transaction.begin();
            const t1 = await Transaction.promise.findById(t0._id);
            t1.state = 'expire';
            await t1.promise.save();
            try {
                await t0.commit();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.UNKNOWN_COMMIT_ERROR);
            }
            t0.state.should.eql('expire');
        }));

    it('if transaction committed for another process, ' +
        'cannot move expire state',
        ma(async() => {
            const t0 = await Transaction.begin();
            const t1 = await Transaction.promise.findById(t0._id);
            t1.state = 'commit';
            await t1.promise.save();
            try {
                await t0.expire();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.SOMETHING_WRONG);
            }
            t0.state.should.eql('expire');
        }));

    it('if transaction committed for another process' +
        'we use persistent data of mongodb',
        ma(async() => {
            const x = await createSavedTestDoc();
            const t0 = await Transaction.begin();
            await t0.add(x);
            x.num = 3;

            const t1 = await Transaction.promise.findById(t0._id);
            t1._docs = [];
            const y = new Test({num: 2});
            await t1.add(y);
            await t1._commit();
            await t0.commit();

            const nx = await x.getNative();
            should.exists(nx);
            nx.t.should.not.eql(transaction.NULL_OBJECTID);
            nx.num.should.eql(1);

            const ny = await y.getNative();
            should.exists(ny);
            ny.t.should.eql(transaction.NULL_OBJECTID);
            ny.num.should.eql(2);
        }));

    it('if mongodb raise error when transaction commit, ' +
            'automatically move to expire state',
        ma(async() => {
            const t = await Transaction.begin();
            const save = t._moveState;
            let called = false;
            t._moveState = async function(_, __) {
                if (!called) {
                    called = true;
                    throw new Error('something wrong');
                }
                return save.apply(t, arguments);
            };
            try {
                await t.commit();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.UNKNOWN_COMMIT_ERROR);
            }
            t.state.should.eql('expire');
        }));
});

describe('#2 - guarantee sorting order', () => {
    const DataSchema = new mongoose.Schema({
        data: {type: Number, default: 1},
    }, {shardKey: {_id: 1}, autoIndex: true});

    let Data;
    const unordered = _.shuffle(_.range(10));

    before(ma(async() => {
        Data = transaction.TransactedModel(connection, 'Issue_2', DataSchema);
    }));

    beforeEach(ma(async() => {
        for (let i = 0; i < unordered.length; i++) {
            await (new Data({data: unordered[i]})).promise.save();
        }
    }));

    it('fetch insert order', ma(async() => {
        const t = await Transaction.begin();
        const datas = await t.find(Data, {});
        should(datas.map((d) => d.data)).deepEqual(unordered);
        await t.expire();
    }));

    it('fetch with sort order', ma(async() => {
        const t = await Transaction.begin();
        const datas = await t.find(Data, {}, {sort: {data: 1}});
        should(datas.map((d) => d.data)).deepEqual(_.range(10));
        await t.expire();
    }));
});

describe('#4 - support unique index', () => {
    const DataSchema = new mongoose.Schema({
        key: {type: Number, required: true},
        data: {type: Number, default: 1},
    }, {shardKey: {_id: 1}, autoIndex: true});

    DataSchema.index({
        key: 1,
    }, {unique: true, background: false});

    let Data;

    before(ma(async() => {
        Data = transaction.TransactedModel(connection, 'Issue_4', DataSchema);
    }));

    beforeEach(ma(async() => {
        await Data.promise.ensureIndexes();
    }));

    it('should add unique index data', ma(async() => {
        const t = await Transaction.begin();
        const data = new Data();
        data.key = 1;
        await t.add(data);
        await t.commit();
    }));

    it('should pass diff unique values', ma(async() => {
        const t = await Transaction.begin();
        const d0 = new Data();
        const d1 = new Data();
        d0.key = 0;
        d1.key = 1;
        await t.add(d0);
        await t.add(d1);
        await t.commit();
    }));
});

describe('#6 - not match results as query', () => {
    let Data;
    const DataSchema = new mongoose.Schema({
        changable: {type: Number, required: true},
    });

    before(ma(async() => {
        Data = transaction.TransactedModel(connection, 'Issue_6', DataSchema);
    }));

    it('findOne should ignore changed data', ma(async() => {
        await (new Data({changable: 1})).promise.save();
        const process = async() => {
            const t = await Transaction.begin();
            const x = await t.findOne(Data, {changable: 1});
            if (!x) {
                return;
            }
            const orig = x.toObject();
            x.changable = 0;
            await t.commit();
            return orig;
        }
        const ret = _.countBy(await Promise.all([
            process(),
            process(),
        ]), (d) => (d && d.changable));
        should(ret).deepEqual({1: 1, undefined: 1});
    }));

    it('findOne should find another document', ma(async() => {
        await (new Data({changable: 1})).promise.save();
        await (new Data({changable: 1})).promise.save();

        const process = async() => {
            const t = await Transaction.begin();
            const x = await t.findOne(Data, {changable: 1});
            if (!x) {
                return;
            }
            const orig = x.toObject();
            x.changable = 0;
            await t.commit();
            return orig;
        }
        const ret = _.countBy(await Promise.all([
            process(),
            process(),
        ]), (d) => (d && d.changable));
        should(ret).deepEqual({1: 2});
    }));
});
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
