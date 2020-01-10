// mongoose-transaction
// ===========
// `mongoose-transaction` provide transaction feature under multiple mongodb
// connections/documents.
//
'use strict';
const Promise = require('songbird');
const mongoose = require('mongoose');
mongoose.Promise = Promise;

const _ = require('lodash');
const utils = require('./utils');
const atomic = require('./atomic');
const raw = require('./raw');
const TransactionError = require('./error');
const DEFINE = require('./define');
const TransactionSchema = require('./schema');
const ModelMap = require('./modelmap');
const Hook = require('./hook');
const ERROR_TYPE = DEFINE.ERROR_TYPE;

module.exports = {
    TRANSACTION_ERRORS: ERROR_TYPE,
    TransactionError: TransactionError,
};

const DEBUG = utils.DEBUG;

// const VERSION_TRANSACTION = 4;
module.exports.TRANSACTION_COLLECTION =
    TransactionSchema.TRANSACTION_COLLECTION;

module.exports.TRANSACTION_EXPIRE_GAP =
    TransactionSchema.TRANSACTION_EXPIRE_GAP;

module.exports.NULL_OBJECTID = DEFINE.NULL_OBJECTID;

// exports
module.exports.TransactionSchema = TransactionSchema;
// FIXME: rename this plugin
module.exports.bindShardKeyRule = (schema, rule) => {
    if (!rule || !rule.fields || !rule.rule || !rule.initialize ||
            !Object.keys(rule.rule).length ||
            typeof rule.initialize !== 'function') {
        throw new TransactionError(ERROR_TYPE.BROKEN_DATA);
    }
    schema.add(rule.fields);
    schema.options.shardKey = rule.rule;
    schema.options.shardKey.initialize = rule.initialize;
};

const getShardKeyArray = (shardKey) => {
    return Array.isArray(shardKey) ? shardKey : [];
};

// ### TransactedModel._saveWithoutTransaction
// This method replace original `Model.save`
//
// Save action only can succeed document`s t value is NULL_OBJECTID
//
// mongoose version
// - below 3.x replace save
// - above 4.x set `save prehook`
//
// #### Arguments
// * next - :Function: this chain call original save
// * callback - :Function:
//
// #### Callback arguments
// * err
//
// #### Transaction errors
// * 41 - cannot found update target document
// * 42 - conflict another transaction; document locked
// * 43 - conflict another transaction; transaction still alive
const saveWithoutTransaction = async function saveWithoutTransaction(next,
                                                                     callback) {
    if (this.isNew) {
        return next();
    }

    const promise = (async() => {
        await this.promise.validate();
        let delta = this.$__delta();
        if (!delta) {
            return;
        }
        delta = delta[1];
        DEBUG('DELTA', this.collection.name, ':', JSON.stringify(delta));
        const pseudoModel = ModelMap.getPseudoModel(this);
        this.$__reset();
        // manually save document only can `t` value is unset or NULL_OBJECTID
        const query = {
            _id: this._id,
            $or: [
                {t: DEFINE.NULL_OBJECTID},
                {t: {$exists: false}},
            ],
        };
        utils.addShardKeyDatas(pseudoModel, this, query);
        // TODO: need delta cross check
        const checkFields = {t: 1};
        const data = await atomic.findAndModify(pseudoModel.connection,
                                                this.collection, query, delta,
                                                checkFields);

        const hint = {collection: ModelMap.getCollectionName(this),
                      doc: this._id, query: query};
        if (!data) {
            throw new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_1,
                                       hint);
        }
        if (data.t && !DEFINE.NULL_OBJECTID.equals(data.t)) {
            throw new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_2,
                                       hint);
        }
        this.emit('transactedSave');
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

module.exports.plugin = (schema) => {
    schema.add({t: {type: mongoose.Schema.Types.ObjectId,
                    'default': DEFINE.NULL_OBJECTID}});
    schema.add({__new: Boolean});

    if (DEFINE.MONGOOSE_VERSIONS[0] === 4) {
        schema.pre('save', function preSaveHook(next, callback) {
            this._saveWithoutTransaction(next, callback);
        });
    } else {
        // >= 5
        schema.post('init', function postInitHook() {
            const self = this;
            self._oldSave = self.save;
            self.save = async(callback) => {
                if (!callback) {
                    callback = (err) => {
                        if (err) {
                            throw err;
                        }
                    };
                }
                try {
                    await self.promise._oldSave();
                } catch (err) {
                    if (err.message !== ERROR_TYPE.NORMAL) {
                        return callback(err);
                    }
                }
                callback();
            };
        });

        schema.pre('save', async function preSaveHook() {
            const needToReject = !this.isNew;
            await this._saveWithoutTransaction(() => {});
            if (needToReject) {
                throw new TransactionError(ERROR_TYPE.NORMAL);
            }
        });
    }

    schema.methods._saveWithoutTransaction = saveWithoutTransaction;
};

// ## TransactedModel
// TransactedModel will expand or replace mongoose model's query commands.
//
// It also replace `connection.model('Test', TestSchema)`
//
// - all original method mean change;
//   if document's `t` value is not NULL_OBJECTID, will raise transaction error
// - add shortcuts of native query commands; `findNative`, `findOneNative`
//
// | orig method        | retry query                | ignore lock        |
// |--------------------|----------------------------|--------------------|
// | find               | findWithWaitRetry          | findForce          |
// | findOne            | findOneWithWaitRetry       | findOneForce       |
// | findById           | findByIdWithWaitRetry      | findByIdForce      |
// | collection.find    | findNativeWithWaitRetry    | findNativeForce    |
// | collection.findOne | findOneNativeWithWaitRetry | findOneNativeForce |
//
// :Constructor:
//
// #### Arguments
// * connection - :Connection:
// * modelName - :String:
// * schema - :Schema:
module.exports.addCollectionPseudoModelPair =
        ModelMap.addCollectionPseudoModelPair;

const filterTransactedDocs = async(docs, callback) => {
    const promise = (async() => {
        if (!docs) {
            throw new Error('invalid documents');
        }
        const transactionIdDocsMap = {};
        const transactionIds = [];
        const result = {ids: transactionIds, map: transactionIdDocsMap};
        if (docs.toArray) {
            for (const doc of (await docs.promise.toArray())) {
                if (!doc.t || DEFINE.NULL_OBJECTID.equals(doc.t)) {
                    continue;
                }
                transactionIdDocsMap[doc.t] =
                        transactionIdDocsMap[doc.t] || [];
                transactionIdDocsMap[doc.t].push(doc);
                if (!_.includes(transactionIds, doc.t)) {
                    transactionIds.push(doc.t);
                }
            }
            if (docs.rewind) {
                // WARN - MAGIC!! avoid cursor is exhausted
                if (DEFINE.MONGOOSE_VERSIONS[0] === 4 &&
                        DEFINE.MONGOOSE_VERSIONS[1] <= 7) {
                    await docs.promise.hasNext();
                }
                docs.rewind();
            }
        } else if (docs.forEach) {
            docs.forEach((doc) => {
                if (!doc || !doc.t || DEFINE.NULL_OBJECTID.equals(doc.t)) {
                    return;
                }
                transactionIdDocsMap[doc.t] =
                    transactionIdDocsMap[doc.t] || [];
                transactionIdDocsMap[doc.t].push(doc);
                if (_.includes(transactionIds, doc.t)) {
                    return;
                }
                transactionIds.push(doc.t);
            });
        }
        return result;
    })();

    if (callback) {
        return promise.then((ret) => callback(null, ret)).catch(callback);
    }
    return promise;
};

const recheckTransactions = async(model, transactedDocs, callback) => {
    const transactionIds = transactedDocs.ids;
    const transactionIdDocsMap = transactedDocs.map;

    const promise = (async() => {
        for (let i = 0; i < transactionIds.length; i += 1) {
            const transactionId = transactionIds[i];
            const query = {_id: transactionId};
            TransactionSchema.attachShardKey(query);
            const transactionModel = ModelMap.getPseudoModel(
                TransactionSchema.RAW_TRANSACTION_COLLECTION,
            );
            const Model = transactionModel
                .connection
                .models[TransactionSchema.TRANSACTION_COLLECTION];
            const tr = await Model.promise.findOne(query);
            if (tr && tr.state !== 'done') {
                await tr._postProcess();
                continue;
            }

            const docs = transactionIdDocsMap[transactionId];
            const promises = docs.map(async(doc) => {
                const pseudoModel = ModelMap.getPseudoModel(model);
                const query = {_id: doc._id, t: doc.t};
                utils.addShardKeyDatas(pseudoModel, doc, query);
                if (doc.__new) {
                    await raw.remove(model.collection, query);
                    return;
                }
                const updateQuery = {$set: {t: DEFINE.NULL_OBJECTID}};
                await atomic.releaseLock(pseudoModel.connection.db,
                                         model.collection.name,
                                         query, updateQuery);
            });

            await Promise.all(promises);
        }
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

// ### TransactedModel.find...
// Document during query, if conflict the another transaction,
// method will raise transaction error
//
// But, if document transaction is already finished,
// method will process post actions and retry find document
//
// #### Arguments
// SeeAlso :Model:
//
// #### Callback arguments
// * err
// * doc - :Document: or :Array:
//
// #### Transaction errors
// * 50 - unknown colllection
// * 70 - Exceed retry limit
// SeeAlso :Transaction._postProcess:
const find = (proto, ignoreCallback) => {
    const orig = utils.promisify(proto.target, proto.orig);
    return async function _find(...args) {
        let callback;
        if (!ignoreCallback) {
            callback = args[args.length - 1];
            if (!callback || typeof callback !== 'function') {
                // using special case;
                //   `Model.find({}).sort({}).exec(function() {})`
                // FIXME: support this case
                throw new Error('TRANSACTION_FIND_NOT_SUPPORT_QUERY_CHAIN');
            }
            args.pop();
        }
        const promise = (async() => {
            if (args.length > 1) {
                const pseudoModel = ModelMap.getPseudoModel(proto.model);
                const _defaultFields =
                        ['t'].concat(getShardKeyArray(pseudoModel.shardKey));
                args[1] = utils.setDefaultFields(args[1], _defaultFields);
            }

            const RETRY_LIMIT = 10;
            for (let i = 0; i < RETRY_LIMIT; i += 1) {
                let docs = await orig(...args);
                if (!proto.isMultiple) {
                    docs = [docs];
                }
                const transactedDocs = await filterTransactedDocs(docs);
                if (!transactedDocs.ids.length) {
                    // FIXME need return
                    return proto.isMultiple ? docs : docs[0];
                }
                await recheckTransactions(proto.model, transactedDocs);
            }
            const hint = {collection: ModelMap.getCollectionName(proto.model),
                          query: args.length > 1 ? args[0] : {}};
            throw new TransactionError(ERROR_TYPE.INFINITE_LOOP, hint);
        })();

        if (callback) {
            return promise.then((ret) => callback(null, ret)).catch(callback);
        }
        return promise;
    };
};

// ### TransactedModel.find...Force
// Same action original method
//
// This method will ignore transaction lock
//
// #### Arguments
// SeeAlso :Model:
//
// #### Callback arguments
// * err
// * doc - :Document: or :Array:
const findForce = (proto) => {
    return (...args) => {
        return proto.orig.apply(proto.target, args);
        // TODO delete save method
    };
};

// ### TransactedModel.find...
// Document during query, if conflict the another transaction,
// method will wait-and-retry a number of times.
//
// #### Arguments
// SeeAlso :Model:
//
// #### Callback arguments
// * err
// * doc - :Document: or :Array:
//
// #### Transaction errors
// SeeAlso :TransactedModel.find...:
const findWaitUnlock = (proto) => {
    const _find = find(proto, true);
    return async function __find(...args) {
        const callback = args[args.length - 1];

        if (!callback || typeof callback !== 'function') {
            // using special case;
            //   `Model.find({}).sort({}).exec(function() {})`
            // FIXME: support this case
            throw new Error('TRANSACTION_FIND_NOT_SUPPORT_QUERY_CHAIN');
        }
        args.pop();

        const promise = (async() => {
            if (args.length > 1) {
                const pseudoModel = ModelMap.getPseudoModel(proto.model);
                const _defaultFields =
                        ['t'].concat(getShardKeyArray(pseudoModel.shardKey));
                args[1] = utils.setDefaultFields(args[1], _defaultFields);
            }

            const RETRY_LIMIT = 5;
            let lastError;
            for (let i = 0; i < RETRY_LIMIT; i += 1) {
                try {
                    const docs = await _find(...args);
                    return docs;
                } catch (e) {
                    lastError = e;
                }
                await utils.sleep(_.sample([37, 59, 139]));
            }
            throw lastError;
        })();

        if (callback) {
            return promise.then((ret) => callback(null, ret)).catch(callback);
        }
        return promise;
    };
};

module.exports.TransactedModel = (connection, modelName, schema) => {
    schema.plugin(module.exports.plugin);
    const model = connection.model(modelName, schema);
    const hooks = model._hooks = new Hook();

    model.pre = (...args) => hooks.pre(...args);
    model.post = (...args) => hooks.post(...args);
    model.finalize = (...args) => hooks.finalize(...args);
    model._doHooks = (...args) => hooks.clone().doHooks(...args);

    ModelMap.addCollectionPseudoModelPair(model.collection.name, connection,
                                          schema);

    const toJSON = model.prototype.toJSON;
    model.prototype.toJSON = function transactedModelToJSON(options) {
        const res = toJSON.call(this, options);
        if (res.t && DEFINE.NULL_OBJECTID.equals(res.t)) {
            delete res.t;
        }
        return res;
    };

    const prototypes = [
        {
            target: model,
            name: 'find',
            alternative: null,
            isMultiple: true,
        },
        {
            target: model,
            name: 'findOne',
            alternative: null,
            isMultiple: false,
        },
        {
            target: model.collection,
            name: 'find',
            alternative: 'findNative',
            isMultiple: true,
        },
        {
            target: model.collection,
            name: 'findOne',
            alternative: 'findOneNative',
            isMultiple: false,
        },
    ];

    prototypes.forEach((proto) => {
        const methodName = proto.alternative || proto.name;
        proto.model = model;
        proto.orig = proto.target[proto.name];
        // FIXME
        proto.target[proto.name + 'Force'] = proto.orig;
        const wait = findWaitUnlock(proto);
        const force = findForce(proto);
        model[methodName] = (...args) => wait.apply(model, args);
        model[methodName + 'Force'] = (...args) => force.apply(model, args);
    });

    // syntactic sugar
    ['', 'Force'].forEach(function _sugar(lock) {
        model['findById' + lock] = function __sugar(...args) {
            args[0] = {_id: args[0]};
            return this['findOne' + lock].apply(this, args);
        };
    });

    return model;
};
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
