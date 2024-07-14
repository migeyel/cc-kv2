import { CacheMap } from "./CacheMap";
import { ConfigEntryComponent } from "./ConfigPageComponent";
import { PageAllocatorComponent } from "./PageAllocatorComponent";
import { RecordLog } from "./RecordLog";
import { BTreeComponent, KvPair } from "./btree/Node";
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
import { CowCollection } from "./store/CowStore";
import { MappedCollection } from "./store/MappedStore";

/** A store namespace assignment for a KV store. */
type KvStoreNamespaces = {
    /** Stores the WAL. */
    log: Namespace,

    /** Stores the config page. */
    config: Namespace,

    /** Stores the record header page. */
    headers: Namespace,

    /** Stores the record data pages. */
    pages: Namespace,

    /** Stores the B-Tree leaf nodes. */
    leaves: Namespace,

    /** Stores the B-Tree branch nodes. */
    branches: Namespace,
};

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

    private numTransactions = 0;
    private transactions = new LuaTable<TxId, Transaction>();

    public constructor(
        coll: IStoreCollection<IPage, IPageStore<IPage>>,
        namespaces: KvStoreNamespaces,
    ) {
        this.log = new RecordLog(coll.getStore(namespaces.log as Namespace));
        const btree = new BTreeComponent(
            coll,
            new RecordsComponent(
                coll,
                new PageAllocatorComponent(
                    new ConfigEntryComponent(
                        namespaces.config,
                        ConfigKeys.RECORDS_ALLOCATOR_NUM_PAGES,
                    ),
                    namespaces.pages,
                ),
                namespaces.headers,
            ),
            new ConfigEntryComponent(
                namespaces.config,
                ConfigKeys.BTREE_ROOT,
            ),
            new PageAllocatorComponent(
                new ConfigEntryComponent(
                    namespaces.config,
                    ConfigKeys.LEAVES_ALLOCATOR_NUM_PAGES,
                ),
                namespaces.leaves,
            ),
            new PageAllocatorComponent(
                new ConfigEntryComponent(
                    namespaces.config,
                    ConfigKeys.BRANCHES_ALLOCATOR_NUM_PAGES,
                ),
                namespaces.branches,
            ),
        );

        // Magib numbers :^)
        this.config = new SetEntryConfig(new CacheMap(32), btree);
        this.kvlm = new KvLockManager(btree);
        this.cl = new TxCollection(this.log, coll, this.config, 8, 32);
    }

    /** Returns the number of active transactions. */
    public getNumTransactions(): number {
        return this.numTransactions;
    }

    /** Rolls back all active transactions and closes the database. */
    public close() {
        for (const [_, v] of this.transactions) { v.rollback(); }
        this.numTransactions = 0;
        this.log.close();
    }

    /** Begins a new transaction. */
    public begin(): Transaction {
        this.numTransactions += 1;
        const max = this.transactions.length();
        let txId = math.random(0, max) as TxId;
        if (this.transactions.has(txId)) { txId = max + 1 as TxId; }

        const out = new Transaction(
            txId,
            this.cl,
            this.config,
            this.kvlm,
            () => {
                this.transactions.delete(txId);
                this.numTransactions--;
            },
        );

        this.transactions.set(txId, out);
        return out;
    }

    /** Fetches the next entry greater-than or equal to a key. */
    public rawNext(key: string): KvPair | undefined {
        return this.config.btree.search(this.cl, key)[1];
    }
}

const dirKvStoreNamespaces = {
    log: 0 as Namespace,
    config: 2 as Namespace,
    headers: 4 as Namespace,
    pages: 6 as Namespace,
    leaves: 8 as Namespace,
    branches: 10 as Namespace,
};

const dirKvStoreCowNamespaces = {
    log: 1 as Namespace,
    config: 3 as Namespace,
    headers: 5 as Namespace,
    pages: 7 as Namespace,
    leaves: 9 as Namespace,
    branches: 11 as Namespace,
};

const nsMap = new LuaMap<Namespace, Namespace>();
nsMap.set(dirKvStoreNamespaces.log, dirKvStoreCowNamespaces.log);
nsMap.set(dirKvStoreNamespaces.config, dirKvStoreCowNamespaces.config);
nsMap.set(dirKvStoreNamespaces.headers, dirKvStoreCowNamespaces.headers);
nsMap.set(dirKvStoreNamespaces.pages, dirKvStoreCowNamespaces.pages);
nsMap.set(dirKvStoreNamespaces.leaves, dirKvStoreCowNamespaces.leaves);
nsMap.set(dirKvStoreNamespaces.branches, dirKvStoreCowNamespaces.branches);

export class DirKvStore {
    private lock: DirLock;
    private indexedColl: IndexedCollection;
    private cowColl: CowCollection;
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

        this.clearSnapshotColl();
        this.cowColl = new CowCollection(this.indexedColl);

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
        this.dataDirs.delete(this.getName(dir));
        fs.delete(dir);
    }

    public listDataDirs(): string[] {
        const out = [];
        for (const [_, v] of this.dataDirs) { out.push(v); }
        return out;
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

    public getNumTransactions(): number {
        return this.kvs?.getNumTransactions() || 0;
    }

    public getConfig(key: string): string | undefined {
        return this.indexedColl.getConfig(key);
    }

    public setConfig(key: string, value?: string): void {
        return this.indexedColl.setConfig(key, value);
    }

    public open() {
        this.kvs = new GenericKvStore(this.cowColl, dirKvStoreNamespaces);
    }

    public close(): void {
        this.kvs?.close();
        this.indexedColl.close();
        this.lock.release();
    }

    private clearSnapshotColl() {
        for (const [_, namespace] of pairs(dirKvStoreCowNamespaces)) {
            const store = this.indexedColl.getStore(namespace);
            for (const pageNum of store.listPages()) {
                store.getPage(pageNum).delete();
            }
        }
    }

    public openSnapshot(): GenericKvStore {
        const map = new LuaMap<Namespace, IPageStore<IPage>>();
        for (const [ns1, ns2] of nsMap) {
            map.set(ns1, this.indexedColl.getStore(ns2));
        }

        const mapped = new MappedCollection(map);
        const snapshot = this.cowColl.snapshot(mapped);
        const out = new GenericKvStore(snapshot, dirKvStoreNamespaces);
        return out;
    }

    public closeSnapshot(snapshot: GenericKvStore) {
        snapshot.close();
        this.cowColl.detach();
        this.clearSnapshotColl();
    }

    public rawNext(key: string): KvPair | undefined {
        return assert(this.kvs).rawNext(key);
    }

    public begin(): Transaction {
        return assert(this.kvs).begin();
    }
}
