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
import { IndexedCollection } from "./store/indexed/IndexedStore";
import { uuid4 } from "./uid";

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

class InnerKvStore {
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

/** How a substore is made to be discovered. */
enum SubstoreDescType {
    /** Stored on the hdd drive with a path. */
    HDD_WITH_PATH = 1,

    /** Stored in a disk drive with a UUID directory at its root. */
    DISK_WITH_UUID = 2,
}

const loader = (desc: string): DirStoreCollection => {
    const byte = string.byte(desc);
    if (byte == SubstoreDescType.HDD_WITH_PATH) {
        const path = string.sub(desc, 2);
        assert(fs.isDir(path), "couldn't load substore at " + path);
        return new DirStoreCollection(path, 4096 as PageSize);
    } else if (byte == SubstoreDescType.DISK_WITH_UUID) {
        const uuid = string.sub(desc, 2);
        const path = fs.find("/disk*/" + uuid)[0];
        assert(fs.isDir(path), "couldn't load disk substore " + uuid);
        return new DirStoreCollection(path, 4096 as PageSize);
    } else {
        throw "unrecognized substore type: " + byte;
    }
};

export class KvStore {
    private lock: DirLock;
    private indexedColl: IndexedCollection;
    private inner?: InnerKvStore;

    public constructor(dir: string) {
        this.lock = assert(DirLock.tryAcquire(dir), "database is locked")[0];
        const dataDir = fs.combine(dir, "data");
        this.indexedColl = new IndexedCollection(
            4096 as PageSize,
            new DirStoreCollection(dataDir, 4096 as PageSize),
            loader,
        );
    }

    public addHddSubstore(dir: string, quota: number) {
        fs.makeDir(dir);
        const desc = string.char(SubstoreDescType.HDD_WITH_PATH) + dir;
        this.indexedColl.addSubstore(desc, quota);
    }

    public addDriveSubstore(driveName: string, quota: number) {
        const types = peripheral.getType(driveName);
        assert(types.indexOf("drive") != -1, "not a drive: " + driveName);
        const drive = peripheral.wrap(driveName) as DrivePeripheral;
        const uuid = uuid4();
        fs.makeDir(fs.combine(drive.getMountPath(), uuid));
        const desc = string.char(SubstoreDescType.DISK_WITH_UUID) + uuid;
        this.indexedColl.addSubstore(desc, quota);
    }

    public delHddSubstore(dir: string) {
        const desc = string.char(SubstoreDescType.HDD_WITH_PATH) + dir;
        this.indexedColl.delSubstore(desc);
        fs.delete(dir);
    }

    public delDriveSubstore(driveName: string) {
        const types = peripheral.getType(driveName);
        assert(types.indexOf("drive") != -1, "not a drive: " + driveName);
        const drive = peripheral.wrap(driveName) as DrivePeripheral;
        const mountPath = drive.getMountPath();
        for (const name of fs.list(mountPath)) {
            const desc = string.char(SubstoreDescType.DISK_WITH_UUID) + name;
            if (this.indexedColl.getSubstore(desc)) {
                this.indexedColl.delSubstore(desc);
                fs.delete(fs.combine(mountPath, desc));
            }
        }
    }

    public setDriveSubstoreQuota(driveName: string, quota: number) {
        const types = peripheral.getType(driveName);
        assert(types.indexOf("drive") != -1, "not a drive: " + driveName);
        const drive = peripheral.wrap(driveName) as DrivePeripheral;
        const mountPath = drive.getMountPath();
        for (const name of fs.list(mountPath)) {
            const desc = string.char(SubstoreDescType.DISK_WITH_UUID) + name;
            if (this.indexedColl.getSubstore(desc)) {
                this.indexedColl.setSubstoreQuota(desc, quota);
            }
        }
    }

    public getDriveSubstoreQuota(driveName: string): number | undefined {
        const types = peripheral.getType(driveName);
        assert(types.indexOf("drive") != -1, "not a drive: " + driveName);
        const drive = peripheral.wrap(driveName) as DrivePeripheral;
        const mountPath = drive.getMountPath();
        for (const name of fs.list(mountPath)) {
            const desc = string.char(SubstoreDescType.DISK_WITH_UUID) + name;
            const substore = this.indexedColl.getSubstore(desc);
            if (substore) {
                return substore.quota;
            }
        }
    }

    public getDriveSubstoreUsage(driveName: string): number | undefined {
        const types = peripheral.getType(driveName);
        assert(types.indexOf("drive") != -1, "not a drive: " + driveName);
        const drive = peripheral.wrap(driveName) as DrivePeripheral;
        const mountPath = drive.getMountPath();
        for (const name of fs.list(mountPath)) {
            const desc = string.char(SubstoreDescType.DISK_WITH_UUID) + name;
            const substore = this.indexedColl.getSubstore(desc);
            if (substore) {
                return substore.usage;
            }
        }
    }

    public close(): void {
        this.inner?.close();
        this.indexedColl.close();
        this.lock.release();
    }

    public begin(): Transaction {
        if (!this.inner) { this.inner = new InnerKvStore(this.indexedColl); }
        return this.inner.begin();
    }
}
