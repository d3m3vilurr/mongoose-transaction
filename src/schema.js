// ## Transaction
// ### State
// All transaction start within `pending` state.
// It will delete after `commit` or `expire` states.
//
//         pending (create transaction document)
//            |
//         .----.
//         v    v
//      commit expire
//         |    |
//         `----'
//            |
//            v
//          done (delete transaction document)
//
// ### Legacy
// this project has some legacy codes. These codes need to care
// mongo/mongoose's buggy actions
//
// #### findAndModify
// Transaciton require `mongo native atomic` actions, so we only can use
// `findAndModify` or `update` command.
//
// but `findAndModify` command will lock of all shard collections,
// make `pseudo findAndModify` using combine two commands(`update`, `findOne`)
//
// #### update
// Some `specific` mongodb's `update` command return buggy result.
//
// - data write finished, `update` return updated documents count is 0.
// - mongoose use this count, it raise error
//
// So, we decide if update result count is 0, send `findOne` command,
// recheck document change.
'use strict';
const Promise = require('songbird');
const mongoose = require('mongoose');
const pluralize = require('mongoose-legacy-pluralize');
const _ = require('lodash');
const atomic = require('./atomic');
const utils = require('./utils');
const TransactionError = require('./error');
const DEFINE = require('./define');
const ModelMap = require('./modelmap');
const Hook = require('./hook');
const ERROR_TYPE = DEFINE.ERROR_TYPE;
const DEBUG = utils.DEBUG;
const ONE_MINUTE = 60 * 1000;

const TransactionSchema = new mongoose.Schema({
    history: [],
    state: {type: String, required: true, 'default': 'pending'},
});

TransactionSchema.TRANSACTION_COLLECTION = 'Transaction';
TransactionSchema.RAW_TRANSACTION_COLLECTION =
    pluralize(TransactionSchema.TRANSACTION_COLLECTION);
TransactionSchema.TRANSACTION_EXPIRE_GAP = ONE_MINUTE;

TransactionSchema.attachShardKey = (doc) => {
    if (!TransactionSchema.options.shardKey ||
            !TransactionSchema.options.shardKey.initialize) {
        return;
    }
    TransactionSchema.options.shardKey.initialize(doc);
};

// ### Transaction.new
// Create new transaction
//
// :staticmethod:
//
// #### Arguments
// * callback - :Function:
//
// #### Callback arguments
// * err
// * doc - :Document:
TransactionSchema.statics.new =
TransactionSchema.statics.begin = async function beginTransaction(callback) {
    const TransactionModel = this;
    const promise = (async() => {
        const transaction = new TransactionModel();
        await transaction.begin();
        return transaction;
    })();

    if (callback) {
        return promise.then((ret) => callback(null, ret)).catch(callback);
    }
    return promise;
};

// ### Transaction.isExpired
// Check transaction expiring
//
// If old transaction, process decide `deadlock`
//
// #### Return
// :Boolean:
TransactionSchema.methods.isExpired = function isExpired() {
    const issued = this._id.getTimestamp();
    const now = new Date();
    const diff = now - issued;
    return TransactionSchema.TRANSACTION_EXPIRE_GAP < diff;
};

// ### Transaction.begin
// Start transaction mode
//
// Save transaction document with `pending` state to mongodb
//
// #### Arguments
// * callback - :Function:
//
// #### Callback arguments
// * err
TransactionSchema.methods.begin = async function begin(callback) {
    this.state = 'pending';
    this._docs = [];

    const promise = (async() => {
        DEBUG('transaction begin', this._id);
        TransactionSchema.attachShardKey(this);
        await this.promise.save();
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

// ### Transaction.add
// Target document marking for transact manage
//
// Modify `t` value of document and save to mongodb
//
// #### Arguments
// * doc - :Document:
// * callback - :Function:
//
// #### Callback arguments
// * err
//
// #### Transaction errors
// * 40 - document already has `t` value, value is not match transaction's id
// * 50 - unknown colllection
//
// SeeAlso :atomic.acquireTransactionLock:
TransactionSchema.methods.add = async function addDoc(doc, callback) {
    const promise = (async() => {
        if (Array.isArray(doc)) {
            const promises = doc.map(async(d) => this.add(d));
            await Promise.all(promises);
            return;
        }

        const pseudoModel = ModelMap.getPseudoModel(doc);
        TransactionSchema.attachShardKey(this);

        await doc.promise.validate();
        if (doc.isNew) {
            // create new document
            const data = {_id: doc._id, t: this._id, __new: true};
            utils.addShardKeyDatas(pseudoModel, doc, data);
            utils.addUniqueKeyDatas(pseudoModel, doc, data);
            await doc.collection.promise.insert(data);
            this._docs.push(doc);
            return;
        }
        if (doc.t && !DEFINE.NULL_OBJECTID.equals(doc.t) &&
                doc.t.toString() !== this._id.toString()) {
            const hint = {collection: doc && ModelMap.getCollectionName(doc),
                          doc: doc && doc._id, transaction: this};
            throw new TransactionError(ERROR_TYPE.BROKEN_DATA, hint);
        }
        if (doc.t && !DEFINE.NULL_OBJECTID.equals(doc.t)) {
            // FIXME: preload transacted
            return;
        }
        const query = {
            _id: doc._id,
            $or: [
                {t: DEFINE.NULL_OBJECTID},
                {t: {$exists: false}},
            ],
        };
        utils.addShardKeyDatas(pseudoModel, doc, query);
        const update = {$set: {t: this._id}};
        await atomic.acquireLock(pseudoModel.connection.db,
                                 doc.collection.name, query, update);
        this._docs.push(doc);
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

// ### Transaction.removeDoc
// Target document making to remove
//
// #### Arguments
// * doc - :Document:
// * callback - :Function:
//
// #### Callback arguments
// * err
//
// #### Transaction errors
// SeeAlso :Transaction.add:
TransactionSchema.methods.removeDoc = async function removeDoc(doc, callback) {
    const promise = (async() => {
        ModelMap.getPseudoModel(doc);
        doc.isRemove = true;
        await this.add(doc);
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

// ### Transaction._moveState
// Change transaction state
//
// **`prev` is current state in db
// **`delta` must have `state: 'next_state'`
//
// #### Arguments
// * prev - :String:
// * delta - :Object:
// * callback - :Function:
//
// #### Return
// :Promise:
//
// #### Transaction errors
// * 41 - state already changed; process conflict
TransactionSchema.methods._moveState = async function _moveState(prev, delta) {
    const transactionModel = ModelMap.getPseudoModel(
        TransactionSchema.RAW_TRANSACTION_COLLECTION,
    );
    const query = {_id: this._id, state: prev};
    TransactionSchema.attachShardKey(query);

    const [numberUpdated, collection] = await atomic.pseudoFindAndModify(
        transactionModel.connection.db,
        TransactionSchema.RAW_TRANSACTION_COLLECTION,
        query,
        delta,
    );
    if (numberUpdated === 1) {
        return;
    }
    // if `findAndModify` return wrong result,
    // it only can wrong query case.
    const modQuery = utils.unwrapMongoOp(utils.wrapMongoOp(query));
    delete modQuery.state;

    const updatedDoc = await collection.promise.findOne(modQuery);
    const state1 = delta.state || ((delta.$set || {}).state);
    const state2 = updatedDoc && updatedDoc.state;
    if (state1 !== state2) {
        throw new TransactionError(ERROR_TYPE.SOMETHING_WRONG, {
            transaction: this,
            query: query,
            updated: updatedDoc,
        });
    }
    return (updatedDoc && updatedDoc.history) || [];
};

// ### Transaction.commit
// Commit all changes
//
// #### Arguments
// * callback - :Function:
//
// #### Callback arguments
// * err
TransactionSchema.methods.commit = async function commit(callback) {
    await this._doHooks('pre', 'commit');

    const mainPromise = (async() => {
        await this._commit();
        const errors = [];

        const promises = this.history.map(async(history) => {
            let pseudoModel;
            try {
                pseudoModel = ModelMap.getPseudoModel(history.col);
            } catch (e) {
                errors.push(e);
                return;
            }
            const query = {_id: history.id, t: this._id};
            utils.addShardKeyDatas(pseudoModel, history, query);
            if (history.options && history.options.remove) {
                const col = pseudoModel.connection.collection(history.col);
                try {
                    await col.promise.remove(query);
                } catch (e) {
                    errors.push(e);
                }
                return;
            }
            if (!history.op) {
                await utils.sleep(-1);
                return;
            }
            const op = utils.unwrapMongoOp(JSON.parse(history.op));
            utils.removeShardKeySetData(pseudoModel.shardKey, op);
            try {
                await atomic.releaseLock(pseudoModel.connection.db,
                                         history.col, query, op);
            } catch (e) {
                errors.push(e);
            }
        });
        await Promise.all(promises);

        if (errors.length) {
            // eslint-disable-next-line
            console.error('errors', errors);
            // TODO: cleanup batch
            return;
        }
        await this.collection.promise.remove({_id: this._id});
    })();

    try {
        await mainPromise;
        await this._doHooks('post', 'commit');
    } catch (e) {
        await this._doHooks('finalize');
        if (callback) {
            return callback(e);
        }
        throw e;
    }
    await this._doHooks('finalize');

    if (callback) {
        callback();
    }
};

// ### Transaction.pre
// Add pre hook to this transaction
//
// The argument 'func' is executed before commit/expire of
// current transaction.
// Execution orders of hook functions are NOT guaranteed,
// so they should not have order dependency.
// Awaiting on transaction.commit() will await for completion of
// all pre-commit hooks.
//
// #### Arguments
// * type - :String:
// * func - :Function:
TransactionSchema.methods.pre = function preHook(type, hook) {
    this._getHooks().pre(type, hook);
};

// ### Transaction.post
// Add post hook to this transaction
//
// The argument 'func' is executed after commit/expire of
// current transaction.
// Execution orders of hook functions are NOT guaranteed,
// so they should not have order dependency.
// Awaiting on transaction.commit() will await for completion of
// all post-commit hooks.
//
// #### Arguments
// * type - :String:
// * func - :Function:
TransactionSchema.methods.post = function postHook(type, hook) {
    this._getHooks().post(type, hook);
};

// ### Transaction.finalize
// Add finalize hook to this transaction
//
// The argument 'func' is executed whenever success commit or not.
// Execution orders of hook functions are NOT guaranteed,
// so they should not have order dependency.
// Awaiting on transaction.commit() will await for completion of
// all finalize hooks.
//
// #### Arguments
// * func - :Function:
TransactionSchema.methods.finalize = function finalizeHook(hook) {
    this._getHooks().finalize(hook);
};

TransactionSchema.methods._getHooks = function getHooks() {
    if (!this.hooks) {
        this.hooks = new Hook(true);
    }
    return this.hooks;
};

TransactionSchema.methods._doHooks = async function doHooks(...types) {
    const promises = (this._docs || []).map(async(doc) => {
        try {
            if (doc.constructor && doc.constructor._doHooks) {
                await doc.constructor._doHooks(doc, ...types);
            }
        } catch (e) {
            // FIXME
            // console.log(e);
        }
    });
    promises.push(this._getHooks().doHooks(this, ...types));
    await Promise.all(promises);
};

// ### Transaction.afterCommit
// DEPRECATED: replaced `post` hook
//
// Add after-commit hook to this transaction
//
// The argument 'func' is executed after successful commit of
// current transaction.
// Execution orders of hook functions are NOT guaranteed,
// so they should not have order dependency.
// Awaiting on transaction.commit() will await for completion of
// all after-commit hooks.
//
// #### Arguments
// * func - :Function:
TransactionSchema.methods.afterCommit = function afterCommit(func) {
    const warn = 'mongoose-transaction: DEPRECATED `afterCommit`.'
        + ' Please use `post(\'commit\', hook)` instead this\n';
    process.stderr.write(warn);
    this.post('commit', func);
};

// ### Transaction._makeHistory
// Make `mongodb delta` of all changes
//
// This private method process below actions:
// * all document deltas mark processed
// * collected all deltas store transaction's history fields.
//
// #### Return
// :Promise:
//
// #### Promise arguments
// * errors
TransactionSchema.methods._makeHistory = async function _makeHistory(action) {
    const errors = [];

    if (this.history.length) {
        return errors;
    }

    this._docs = this._docs || [];
    const promises = this._docs.map(async(doc) => {
        let err;
        try {
            await doc.promise.validate();
        } catch (e) {
            err = e;
        }
        const history = {
            col: doc.collection.name,
            id: doc._id,
            options: {
                new: doc.isNew,
                remove: doc.isRemove,
            },
        };
        utils.addShardKeyDatas(ModelMap.getPseudoModel(doc), doc, history);
        this.history.push(history);
        if (err) {
            errors.push(err);
            // no need more action
            return;
        }

        const delta = utils.extractDelta(doc);
        delta.$set = delta.$set || {};
        delta.$set.t = DEFINE.NULL_OBJECTID;

        if (doc.isNew) {
            delta.$set = doc.toObject({depopulate: 1});
            if (!doc.$__.version) {
                doc.$__version(true, delta.$set);
            }
            delete delta.$set._id;
            delta.$unset = {__new: 1};
        }

        DEBUG('DELTA', doc.collection.name, ':', JSON.stringify(delta));
        history.op = delta && JSON.stringify(utils.wrapMongoOp(delta));

        doc.$__reset();
    });

    await Promise.all(promises);

    return errors;
};

// ### Transaction._commit
// Transaction state move to `commit`
//
// This method save :Transaction._makeHistory:'s result
//
// #### Return
// * :Promise:
//
// #### Transaction errors
// * 44 - already expired at another process
// * 60 - unknown error
//
// SeeAlso :Transaction._moveState:
TransactionSchema.methods._commit = async function _commit() {
    if (this.state === 'commit') {
        DEBUG('retry transaction commit', this._id);
        return;
    }

    if (this.state === 'expire') {
        DEBUG('transaction expired', this._id);
        throw new TransactionError(ERROR_TYPE.TRANSACTION_EXPIRED,
                                   {transaction: this});
    }

    const hint = {transaction: this};
    const errors = await this._makeHistory('commit');
    if (errors.length) {
        // eslint-disable-next-line
        console.error(errors);
        await this.expire();
        if (errors[0]) {
            throw errors[0];
        }
        throw new TransactionError(ERROR_TYPE.UNKNOWN_COMMIT_ERROR,
                                   hint);
    }
    if (this.isExpired()) {
        await this.expire();
        throw new TransactionError(ERROR_TYPE.TRANSACTION_EXPIRED, hint);
    }
    this.state = 'commit';
    DEBUG('transaction commit', this._id);
    const delta = utils.extractDelta(this);
    let history;
    try {
        history = await this._moveState('pending', delta);
    } catch (e) {
        // this case only can db error or already expired
        this.state = undefined;
        await this.expire();
        throw new TransactionError(ERROR_TYPE.UNKNOWN_COMMIT_ERROR, hint);
    }
    if (history) {
        this.history = history;
    }
    this.$__reset();
};

// ### Transaction._expire
// Transaction state move to `expire`
//
// This method save :Transaction._makeHistory:'s result
//
// #### Return
// :Promise:
//
// #### Transaction errors
// SeeAlso :Transaction._moveState:
TransactionSchema.methods._expire = async function _expire() {
    DEBUG('transaction expired', this._id);

    if (this.state === 'expire') {
        DEBUG('retry transaction expired', this._id);
        return;
    }
    if (this.state === 'commit') {
        DEBUG('transaction committed', this._id);
        return;
    }
    await this._makeHistory('expire');
    this.state = 'expire';
    const delta = utils.extractDelta(this);
    const history = await this._moveState('pending', delta);
    if (history) {
        this.history = history;
    }
    this.$__reset();
};

// ### Transaction.expire
// Expire transaction
//
// This method mean same `rollback` of common db
//
// #### Arguments
// * callback - :Function:
//
// #### Callback arguments
// * err
TransactionSchema.methods.expire = async function expire(callback) {
    await this._doHooks('pre', 'expire');

    const mainPromise = (async() => {
        await this._expire();
        const errors = [];

        const promises = this.history.map(async(history) => {
            if (!history.op) {
                await utils.sleep(-1);
                return;
            }
            let pseudoModel;
            try {
                pseudoModel = ModelMap.getPseudoModel(history.col);
            } catch (e) {
                errors.push(e);
                return;
            }

            const query = {_id: history.id, t: this._id};
            utils.addShardKeyDatas(pseudoModel, history, query);
            if (history.options && history.options.new) {
                const col = pseudoModel.connection.collection(history.col);
                try {
                    await col.promise.remove(query);
                } catch (e) {
                    errors.push(e);
                }
                return;
            }

            try {
                await atomic.releaseLock(pseudoModel.connection.db,
                                         history.col, query,
                                         {$set: {t: DEFINE.NULL_OBJECTID}});
            } catch (e) {
                errors.push(e);
            }
        });
        await Promise.all(promises);
        if (errors.length) {
            // eslint-disable-next-line
            console.error('errors', errors);
            // TODO: cleanup batch
            return;
        }
        await this.collection.promise.remove({_id: this._id});
    })();

    try {
        await mainPromise;
        await this._doHooks('post', 'expire');
    } catch (e) {
        await this._doHooks('finalize');
        if (callback) {
            return callback(e);
        }
        throw e;
    }
    await this._doHooks('finalize');

    if (callback) {
        callback();
    }
};

// ### Transaction.cancel
// Cancel transaction
//
// inputed `reason` return to callback`s `err`
//
// #### Arguments
// * reason - :String: or :Error:
// * callback - :Function:
//
// #### Callback arguments
// * err
TransactionSchema.methods.cancel = async function cancel(reason, callback) {
    const promise = (async() => {
        await this.expire();
        throw reason;
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

// ### Transaction._postProcess
// If transaction process had problem, continue process
//
// #### Return
// :Promise:
//
// #### Transaction errors
// * 43 - conflict another transaction; transaction still alive
TransactionSchema.methods._postProcess = async function _postProcess() {
    switch (this.state) {
        case 'pending':
            if (this.isExpired()) {
                await this.expire();
                return;
            }
            const hint = {collection: ModelMap.getCollectionName(this),
                          transaction: this};
            throw new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_2,
                                       hint);
        case 'commit':
            await this.commit();
            return;
        case 'expire':
            await this.expire();
            return;
        default:
            throw new TransactionError(ERROR_TYPE.SOMETHING_WRONG);
    }
};

TransactionSchema.methods.convertQueryForAvoidConflict = (conditions) => {
    conditions = conditions || {};
    conditions.$or = conditions.$or || [];
    conditions.$or = conditions.$or.concat([{t: DEFINE.NULL_OBJECTID},
                                            {t: {$exists: false}}]);
    return conditions;
};

TransactionSchema.methods._acquireLock = async function _acquireLock(model, fields,
                                                             query1, query2) {
    let doc;
    const updateQuery = {$set: {t: this._id}};
    const opt = {new: true, fields: fields};
    try {
        // try lock
        doc = await model.promise.findOneAndUpdate(query1, updateQuery,
                                                   opt);
    } catch (e) {}

    if (doc) {
        this._docs.push(doc);
        return doc;
    }

    // if t is not NULL_OBJECTID, try to go end of transaction process
    try {
        // just request data unlock
        await model.promise.findOne(query2);
    } catch (e) {}
};

// ### Transaction.find
// Find documents with assgined `t`
//
// If conflict another process, this method wait and retry action
//
// #### Arguments
// * model - :TransactedModel:
// * conditions - :Object:
// * options - :Object:
// * callback - :Function:
//
// #### Callback arguments
// * err
// * docs - :Array:
//
// #### Transaction errors
// * 43 - conflict another transaction; document locked
// * 50 - unknown collection
TransactionSchema.methods.find = async function find(model, ...args) {
    let callback;
    if (typeof args[args.length - 1] === 'function') {
        callback = args[args.length - 1];
        args.pop();
    }

    let [conditions, options] = args;
    conditions = conditions || {};
    options = options || {};

    if (options.skip || options.limit) {
        console.error('not support skip and limit yet');
    }

    const origOrCondition = conditions.$or = conditions.$or || [];
    conditions.$or = conditions.$or.concat([{t: {$ne: this._id}}]);

    let stillRemain = true;
    const promise = (async() => {
        const pseudoModel = ModelMap.getPseudoModel(model);
        const fields = {_id: 1};
        utils.addShardKeyFields(pseudoModel, fields);
        let RETRY_LIMIT = 5;
        const locked = [];
        while (RETRY_LIMIT--) {
            // TODO: sort
            const cursor = await model.collection.promise.find(conditions,
                                                               fields);
            const docs = await cursor.promise.toArray();
            if (!docs || !docs.length) {
                stillRemain = false;
                break;
            }
            const promises = docs.map(async(doc) => {
                let query1 = {_id: doc._id, $or: [{t: DEFINE.NULL_OBJECTID},
                                                  {t: {$exists: false}}]};
                utils.addShardKeyDatas(pseudoModel, doc, query1);
                query1 = _.defaultsDeep(query1, conditions);
                query1.$or = query1.$or.concat(origOrCondition);
                const query2 = {_id: doc._id, t: {$ne: this._id}};
                utils.addShardKeyDatas(pseudoModel, doc, query2);
                const lockedDoc = await this._acquireLock(model, options.fields,
                                                          query1, query2);
                if (!lockedDoc) {
                    return;
                }
                locked.push(lockedDoc);
            });
            await Promise.all(promises);
        }
        if (stillRemain) {
            const hint = {collection: ModelMap.getCollectionName(model),
                          query: conditions, transaction: this};
            throw new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_2, hint);
        }
        if (!options.sort) {
            options.sort = {_id: 1};
        }
        return locked.sort((a, b) => {
            for (const k in options.sort) {
                if (a[k] === b[k]) {
                    continue;
                }
                return (a[k] < b[k] ? -1 : 1) * options.sort[k];
            }
            return 0;
        });
    })();

    if (callback) {
        return promise.then((ret) => callback(null, ret)).catch(callback);
    }
    return promise;
};

// ### Transaction.findOne
// Find documents with assgined `t`
//
// If conflict another process, this method wait and retry action
//
// #### Arguments
// * model - :TransactedModel:
// * conditions - :Object:
// * options - :Object:
// * callback - :Function:
//
// #### Callback arguments
// * err
// * doc - :Document:
//
// #### Transaction errors
// * 43 - conflict another transaction; transaction still alive
// * 50 - unknown collection
TransactionSchema.methods.findOne = async function findOne(model, ...args) {
    let callback;
    if (typeof args[args.length - 1] === 'function') {
        callback = args[args.length - 1];
        args.pop();
    }

    let [conditions, options] = args;
    conditions = conditions || {};
    options = options || {};

    const RETRY_LIMIT = 5;
    const _findOne = async(retry = 1) => {
        const pseudoModel = ModelMap.getPseudoModel(model);
        const _doc = await model.collection.promise.findOne(conditions,
                                                            options);
        if (!_doc) {
            return;
        }

        let query1 = {_id: _doc._id, $or: [{t: DEFINE.NULL_OBJECTID},
                                           {t: {$exists: false}}]};
        utils.addShardKeyDatas(pseudoModel, _doc, query1);
        query1 = _.defaultsDeep(query1, conditions);
        const query2 = {_id: _doc._id};
        utils.addShardKeyDatas(pseudoModel, _doc, query2);
        const doc = await this._acquireLock(model, options.fields, query1, query2);
        if (doc) {
            return doc;
        }

        if (retry === RETRY_LIMIT) {
            const hint = {collection: _doc && ModelMap.getCollectionName(_doc),
                          doc: _doc && _doc._id, query: query1,
                          transaction: this};
            throw new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_2, hint);
        }
        await utils.sleep(_.sample([37, 59, 139]));
        return await _findOne(retry + 1);
    };

    const promise = _findOne();

    if (callback) {
        return promise.then((ret) => callback(null, ret)).catch(callback);
    }
    return promise;
};

module.exports = TransactionSchema;
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
