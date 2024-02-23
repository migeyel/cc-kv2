import { CacheMap } from "./CacheMap";
import { ConfigEntryComponent } from "./ConfigPageComponent";
import { PageAllocatorComponent } from "./PageAllocatorComponent";
import { RecordLog } from "./RecordLog";
import { BTreeComponent } from "./btree/Node";
import { RecordsComponent } from "./records/Records";
import { DirStoreCollection } from "./store/DirStore";
import {
    IPage,
    IPageStore,
    IStoreCollection,
    Namespace,
    PageSize,
} from "./store/IPageStore";
import {
    TxId,
    TxCollection,
} from "./txStore/LogStore";
import { KvLockManager } from "./lock/KvLockManager";
import DirLock from "./DirLock";
import { SetEntryConfig } from "./SetEntryConfig";
import { Transaction } from "./Transaction";

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

export class InnerKvStore {
    private cl: TxCollection;
    private config: SetEntryConfig;
    private kvlm: KvLockManager;
    private log: RecordLog;

    private transactions = new LuaTable<TxId, Transaction>();

    public constructor(coll: IStoreCollection<IPage, IPageStore<IPage>>) {
        this.log = new RecordLog(coll.getStore(Namespaces.LOG as Namespace));
        const btree = new BTreeComponent(
            coll,
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

export class KvStore {
    private lock: DirLock;
    private inner: InnerKvStore;

    public constructor(dir: string) {
        this.lock = assert(DirLock.tryAcquire(dir), "database is locked")[0];
        const dataDir = fs.combine(dir, "data");
        const coll = new DirStoreCollection(dataDir, 4096 as PageSize);
        this.inner = new InnerKvStore(coll);
    }

    public close(): void {
        this.inner.close();
        this.lock.release();
    }

    public begin(): Transaction {
        return this.inner.begin();
    }
}
