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
import { KvLockManager } from "./transaction/KvLockManager";
import DirLock from "./DirLock";
import { SetEntryConfig } from "./SetEntryConfig";
import { Transaction } from "./transaction/Transaction";
import { IndexedCollection } from "./store/indexed/IndexedStore";

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

class GenericKvStore {
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

export class DirKvStore {
    private lock: DirLock;
    private indexedColl: IndexedCollection;
    private kvs?: GenericKvStore;
    private dataDirs: LuaMap<string, string>;

    public constructor(indexDir: string, dataDirs: string[]) {
        this.dataDirs = new LuaMap();
        for (const dir of dataDirs) {
            const name = fs.getName(dir);
            assert(!this.dataDirs.has(name), "duplicate directory: " + name);
            this.dataDirs.set(name, dir);
        }

        const loader = (desc: string): DirStoreCollection => {
            const [dir] = assert(this.dataDirs.get(desc), "not found: " + desc);
            return new DirStoreCollection(dir, 4096 as PageSize);
        };

        this.lock = assert(DirLock.tryAcquire(indexDir), "database is locked")[0];
        const indexDataDir = fs.combine(indexDir, "data");
        this.indexedColl = new IndexedCollection(
            4096 as PageSize,
            new DirStoreCollection(indexDataDir, 4096 as PageSize),
            loader,
        );

        for (const [name] of this.dataDirs) {
            if (!this.indexedColl.getSubstore(name)) {
                this.dataDirs.delete(name);
            }
        }
    }

    public addDataDir(dir: string, quota: number) {
        const name = fs.getName(dir);
        assert(!this.dataDirs.has(name));
        fs.makeDir(dir);
        this.dataDirs.set(name, dir);
        this.indexedColl.addSubstore(name, quota);
    }

    private getName(dir: string): string {
        const name = fs.getName(dir);
        assert(dir == this.dataDirs.get(name), "wrong data directory: " + dir);
        return name;
    }

    public delDataDir(dir: string) {
        this.indexedColl.delSubstore(this.getName(dir));
        fs.delete(dir);
    }

    public setDataDirQuota(dir: string, quota: number) {
        this.indexedColl.setSubstoreQuota(this.getName(dir), quota);
    }

    public getDataDirQuota(dir: string): number {
        return assert(this.indexedColl.getSubstore(this.getName(dir))).quota;
    }

    public getDataDirUsage(dir: string): number {
        return assert(this.indexedColl.getSubstore(this.getName(dir))).usage;
    }

    public getUsage(): number {
        return this.indexedColl.getUsage();
    }

    public getQuota(): number {
        return this.indexedColl.getQuota();
    }

    public open() {
        this.kvs = new GenericKvStore(this.indexedColl);
    }

    public close(): void {
        this.kvs?.close();
        this.indexedColl.close();
        this.lock.release();
    }

    public begin(): Transaction {
        return assert(this.kvs).begin();
    }
}
