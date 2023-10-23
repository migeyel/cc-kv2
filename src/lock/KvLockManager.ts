import { BTreeComponent, KvPair } from "../btree/Node";
import { TxCollection, TxId } from "../txStore/LogStore";
import { Lock, LockedResource } from "./Lock";

/** A SS2PL lock manager. */
export class KvLockManager {
    /** The lock manager requires a view of uncommitted entries in a B+ Tree. */
    private btree: BTreeComponent;

    /** Content locks are acquired to view an entry's contents. */
    private content = setmetatable(
        new LuaMap<string, LockedResource>(),
        { __mode: "v" },
    );

    /**
     * Fence locks are acquired to inspect the key range starting at an entry
     * and ending at the next entry.
     */
    private fences = setmetatable(
        new LuaMap<string, LockedResource>(),
        { __mode: "v" },
    );

    /** The map of locks held per transaction. */
    private locksPerTx = new LuaMap<TxId, LuaSet<Lock>>();

    /** The map of transactions that hold a lock. */
    private txsPerLock = setmetatable(
        new LuaMap<Lock, LuaSet<TxId>>(),
        { __mode: "k" },
    );

    /** The first fence is the fence that comes before the first entry. */
    private firstFence = new LockedResource();

    public constructor(btree: BTreeComponent) {
        this.btree = btree;
    }

    private getContent(key: string): LockedResource {
        const out = this.content.get(key);
        if (out) { return out; }
        const newOut = new LockedResource();
        this.content.set(key, newOut);
        return newOut;
    }

    private getFence(key?: string): LockedResource {
        if (!key) { return this.firstFence; }
        const out = this.fences.get(key);
        if (out) { return out; }
        const newOut = new LockedResource();
        this.fences.set(key, newOut);
        return newOut;
    }

    private getLocks(txId: TxId): LuaSet<Lock> {
        const out = this.locksPerTx.get(txId);
        if (out) { return out; }
        const newOut = new LuaSet<Lock>();
        this.locksPerTx.set(txId, newOut);
        return newOut;
    }

    private getTransactions(lock: Lock): LuaSet<TxId> {
        const out = this.txsPerLock.get(lock);
        if (out) { return out; }
        const newOut = new LuaSet<TxId>();
        this.txsPerLock.set(lock, newOut);
        return newOut;
    }

    /** Acquires an exclusive lock on a resource or upgrades if possible. */
    private exclusive(resource: LockedResource, txId: TxId): Lock {
        if (!resource.slot || !this.getTransactions(resource.slot).has(txId)) {
            const lock = Lock.exclusive(resource);
            this.getTransactions(lock).add(txId);
            this.getLocks(txId).add(lock);
            return lock;
        } else {
            resource.slot.upgrade();
            return resource.slot;
        }
    }

    /** Acquires a shared lock on a resource. */
    private shared(resource: LockedResource, txId: TxId): Lock {
        if (!resource.slot || !this.getTransactions(resource.slot).has(txId)) {
            const lock = Lock.shared(resource);
            this.getTransactions(lock).add(txId);
            this.getLocks(txId).add(lock);
            return lock;
        }
        return resource.slot;
    }

    /** Releases a lock and unlinks related structures. */
    private release(lock: Lock) {
        lock.release();
        for (const txId of this.getTransactions(lock)) {
            this.getLocks(txId).delete(lock);
        }
        this.txsPerLock.delete(lock);
    }

    /** Releases all locks on a transaction and unlinks related structures. */
    public releaseLocks(txId: TxId): void {
        for (const lock of this.getLocks(txId)) {
            lock.release();
            const transactions = this.getTransactions(lock);
            transactions.delete(txId);
            if (next(transactions)[0] == undefined) {
                this.txsPerLock.delete(lock);
            }
        }
        this.locksPerTx.delete(txId);
    }

    private syncSearch(
        cl: TxCollection,
        key: string,
    ):  LuaMultiReturn<[KvPair | undefined, KvPair | undefined]> {
        const lock = Lock.shared(cl.resource);
        const [prev, next] = this.btree.search(cl, key);
        lock.release();
        return $multi(prev, next);
    }

    private exclusiveFence(
        cl: TxCollection,
        key: string,
        txId: TxId,
        prev?: KvPair,
    ) {
        // Locking may change what the previous node is, so we need to keep
        // trying until it settles.
        let oldPrev = prev;
        let oldLock = this.exclusive(this.getFence(oldPrev?.key), txId);
        while (true) {
            const [newPrev] = this.syncSearch(cl, key);
            if (oldPrev?.key == newPrev?.key) { break; }
            const newLock = this.exclusive(this.getFence(newPrev?.key), txId);
            this.release(oldLock);
            oldLock = newLock;
            oldPrev = newPrev;
        }
    }

    private sharedFence(
        cl: TxCollection,
        key: string,
        txId: TxId,
        prev?: KvPair,
    ) {
        // Locking may change what the previous node is, so we need to keep
        // trying until it settles.
        let oldPrev = prev;
        let oldLock = this.shared(this.getFence(oldPrev?.key), txId);
        while (true) {
            const [newPrev] = this.syncSearch(cl, key);
            if (oldPrev?.key == newPrev?.key) { break; }
            const newLock = this.shared(this.getFence(newPrev?.key), txId);
            this.release(oldLock);
            oldLock = newLock;
            oldPrev = newPrev;
        }
    }

    /** Acquires locks for setting/inserting a value. */
    public acquireSet(cl: TxCollection, key: string, txId: TxId): void {
        this.exclusive(this.getContent(key), txId);

        const [prev, next] = this.syncSearch(cl, key);
        if (!next || next.key != key) {
            // Key doesn't exist, insertion requires acquiring fence locks.
            this.exclusiveFence(cl, key, txId, prev);
        }
    }

    /** Acquires locks for deleting a value. */
    public acquireDelete(cl: TxCollection, key: string, txId: TxId): void {
        this.exclusive(this.getContent(key), txId);

        const [prev, next] = this.syncSearch(cl, key);
        if (next && next.key == key) {
            // Key exists, deletion requires acquiring fence locks.
            this.exclusiveFence(cl, key, txId, prev);
        }
    }

    /** Acquires locks for getting a value. */
    public acquireGet(key: string, txId: TxId): void {
        this.shared(this.getContent(key), txId);
    }

    /** Acquires locks for getting the next key and value. */
    public acquireNext(cl: TxCollection, key: string, txId: TxId): void {
        const [prev, next] = this.syncSearch(cl, key);
        if (!next || next.key != key) {
            // Key doesn't exist, we need to acquire fence locks.
            this.sharedFence(cl, key, txId, prev);

            // Now we can carry on the search and lock the content.
            const [_, cNext] = this.syncSearch(cl, key);
            if (cNext) { this.shared(this.getContent(cNext.key), txId); }
        } else {
            // Key exists so lock the content.
            this.shared(this.getContent(key), txId);
        }
    }
}
