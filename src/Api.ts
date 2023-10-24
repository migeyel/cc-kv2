import { CacheMap } from "./CacheMap";
import { ConfigEntryComponent } from "./ConfigPageComponent";
import { PageAllocatorComponent } from "./PageAllocatorComponent";
import { RecordLog } from "./RecordLog";
import { BTreeComponent } from "./btree/Node";
import { RecordsComponent } from "./records/Records";
import { VarRecordsComponent } from "./records/VarRecords";
import { DirStoreCollection } from "./store/DirStore";
import { Namespace, PageSize } from "./store/IPageStore";
import {
    AnyTxPage,
    CacheKey,
    IConfig,
    IEvent,
    IObj,
    TxId,
    TxCollection,
} from "./txStore/LogStore";
import { KvLockManager } from "./lock/KvLockManager";
import DirLock from "./DirLock";
import * as expect from "cc/expect";

type SetEntryAct = {
    key: string,
    value?: string,
};

class SetEntryConfig implements IConfig {
    public readonly cache: CacheMap<CacheKey, AnyTxPage>;
    public readonly btree: BTreeComponent;

    public constructor(
        cache: CacheMap<CacheKey, AnyTxPage>,
        btree: BTreeComponent,
    ) {
        this.cache = cache;
        this.btree = btree;
    }

    public deserializeObj(ns: Namespace, str?: string): IObj<IEvent> {
        return assert(this.btree.deserializeObj(ns, str));
    }

    public deserializeEv(ns: Namespace, str: string): IEvent {
        return assert(this.btree.deserializeEv(ns, str));
    }

    public doAct(
        act: SetEntryAct,
        collection: TxCollection,
    ): LuaMultiReturn<[string, undefined]> {
        if (act.value) {
            const oldValue = this.btree.insert(collection, act.key, act.value);
            if (oldValue) {
                return $multi(
                    string.pack("<s4s4", act.key, oldValue),
                    undefined,
                );
            } else {
                return $multi(
                    string.pack("<s4", act.key),
                    undefined,
                );
            }
        } else {
            const oldValue = this.btree.delete(collection, act.key);
            if (oldValue) {
                return $multi(
                    string.pack("<s4s4", act.key, oldValue),
                    undefined,
                );
            } else {
                return $multi(
                    string.pack("<s4", act.key),
                    undefined,
                );
            }
        }
    }

    public undoAct(undoInfo: string, collection: TxCollection): undefined {
        const [key, pos] = string.unpack("<s4", undoInfo);
        if (pos <= undoInfo.length) {
            const [value] = string.unpack("<s4", undoInfo, pos);
            this.btree.insert(collection, key, value);
        } else {
            this.btree.delete(collection, key);
        }
    }
}

class Transaction {
    private id: TxId;
    private cl: TxCollection;
    private config: SetEntryConfig;
    private kvlm: KvLockManager;
    private txsMap: LuaTable<TxId, Transaction>;
    private active = true;

    public constructor(
        id: TxId,
        cl: TxCollection,
        config: SetEntryConfig,
        kvlm: KvLockManager,
        txsMap: LuaTable<TxId, Transaction>,
    ) {
        this.id = id;
        this.cl = cl;
        this.config = config;
        this.kvlm = kvlm;
        this.txsMap = txsMap;
    }

    /**
     * Gets a value.
     * @param key - The key to get.
     * @returns The value matching the key.
     */
    public get(key: string): string | undefined {
        expect(1, key, "string");
        assert(this.active, "can't operate on an inactive transaction");
        this.kvlm.acquireGet(key, this.id);
        const [_, pair] = this.config.btree.search(this.cl, key);
        if (pair && pair.key == key) { return pair.value; }
    }

    /**
     * Returns the next key-value pair starting from a key.
     * @param key - A key to fetch the next pair from.
     * @returns A key-value pair coming after the given key, if any.
     */
    public next(key?: string): LuaMultiReturn<[string, string] | []> {
        expect(1, key, "string", "nil");
        assert(this.active, "can't operate on an inactive transaction");
        const nextSmallestKey = key ? key + "\0" : "";
        this.kvlm.acquireNext(this.cl, nextSmallestKey, this.id);
        const [_, pair] = this.config.btree.search(this.cl, nextSmallestKey);
        if (pair) {
            return $multi(pair.key, pair.value);
        } else {
            return $multi();
        }
    }

    /**
     * Iterates over keys.
     * @param start - A starting key, or nothing to start from the smallest.
     * @returns An iterator over all keys starting from the argument.
     */
    public iter(
        start?: string,
    ): LuaIterable<LuaMultiReturn<[string, string] | []>, Transaction> {
        expect(1, start, "string", "nil");
        // @ts-expect-error: Just go for it.
        return $multi(
            this.next,
            this,
            start,
        );
    }

    /**
     * Sets a key.
     * @param key - The key to set.
     * @param value - The value to set to.
     */
    public set(key: string, value: string): void {
        expect(1, key, "string");
        expect(2, value, "string");
        assert(this.active, "can't operate on an inactive transaction");
        this.kvlm.acquireSet(this.cl, key, this.id);
        this.cl.doAct(this.id, <SetEntryAct>{ key, value });
    }

    /**
     * Deletes a key.
     * @param key - The key to delete.
     */
    public delete(key: string): void {
        expect(1, key, "string");
        assert(this.active, "can't operate on an inactive transaction");
        this.kvlm.acquireDelete(this.cl, key, this.id);
        this.cl.doAct(this.id, <SetEntryAct>{ key });
    }

    /** Commits the transaction. */
    public commit(): void {
        assert(this.active, "can't operate on an inactive transaction");
        this.cl.commit(this.id);
        this.kvlm.releaseLocks(this.id);
        this.active = false;
        this.txsMap.delete(this.id);
    }

    /** Rolls the transaction back. */
    public rollback(): void {
        if (!this.active) { return; }
        this.cl.rollback(this.id);
        this.kvlm.releaseLocks(this.id);
        this.active = false;
        this.txsMap.delete(this.id);
    }
}

enum Namespaces {
    LOG,
    CONFIG,
    HEADERS,
    PAGES,
    LEAVES,
    BRANCHES,
}

enum ConfigKeys {
    RECORDS_ALLOCATOR_NUM_PAGES,
    LEAVES_ALLOCATOR_NUM_PAGES,
    BRANCHES_ALLOCATOR_NUM_PAGES,
    BTREE_ROOT,
}

export class KvStore {
    private cl: TxCollection;
    private config: SetEntryConfig;
    private kvlm: KvLockManager;
    private log: RecordLog;
    private lock: DirLock;

    private transactions = new LuaTable<TxId, Transaction>();

    public constructor(dir: string) {
        this.lock = assert(DirLock.tryAcquire(dir), "database is locked")[0];
        const dataDir = fs.combine(dir, "data");
        const coll = new DirStoreCollection(dataDir, 4096 as PageSize);
        this.log = new RecordLog(coll.getStore(Namespaces.LOG as Namespace));
        const btree = new BTreeComponent(
            coll,
            new VarRecordsComponent(
                new RecordsComponent(
                    coll,
                    new PageAllocatorComponent(
                        new ConfigEntryComponent(
                            Namespaces.CONFIG as Namespace,
                            ConfigKeys.RECORDS_ALLOCATOR_NUM_PAGES,
                        ),
                        Namespaces.PAGES as Namespace,
                    ),
                    Namespaces.HEADERS as Namespace,
                ),
                20,
            ),
            new ConfigEntryComponent(
                Namespaces.CONFIG as Namespace,
                ConfigKeys.BTREE_ROOT as Namespace,
            ),
            new PageAllocatorComponent(
                new ConfigEntryComponent(
                    Namespaces.CONFIG as Namespace,
                    ConfigKeys.LEAVES_ALLOCATOR_NUM_PAGES,
                ),
                Namespaces.LEAVES as Namespace,
            ),
            new PageAllocatorComponent(
                new ConfigEntryComponent(
                    Namespaces.CONFIG as Namespace,
                    ConfigKeys.BRANCHES_ALLOCATOR_NUM_PAGES,
                ),
                Namespaces.BRANCHES as Namespace,
            ),
        );

        // Magib numbers :^)
        this.config = new SetEntryConfig(new CacheMap(32), btree);
        this.kvlm = new KvLockManager(btree);
        this.cl = new TxCollection(this.log, coll, this.config, 8, 32);
    }

    /** Rolls back all active transactions and closes the database. */
    public close() {
        for (const [_, v] of this.transactions) { v.rollback(); }
        this.log.close();
        this.lock.release();
    }

    /** Begins a new transaction. */
    public begin(): Transaction {
        const max = this.transactions.length();
        let txId = math.random(0, max) as TxId;
        if (this.transactions.has(txId)) { txId = max + 1 as TxId; }

        const out = new Transaction(
            txId,
            this.cl,
            this.config,
            this.kvlm,
            this.transactions,
        );

        this.transactions.set(txId, out);
        return out;
    }
}
