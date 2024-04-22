import { BTreeComponent, KvPair } from "../btree/Node";
import { TxCollection } from "../txStore/LogStore";
import { Lock, LockHolder, LockedResource } from "./Lock";

/** A SS2PL lock manager. */
export class KvLockManager {
    /** The lock manager requires a view of uncommitted entries in a B+ Tree. */
    private btree: BTreeComponent;

    /** Content locks are acquired to view an entry's contents. */
    private content = new LuaMap<string, LockedResource>();

    /**
     * Fence locks are acquired to inspect the key range starting at an entry
     * and ending at the next entry.
     */
    private fences = new LuaMap<string, LockedResource>();

    /** The map of locks held per transaction. */
    private locksPerTx = new LuaMap<LockHolder, LuaSet<Lock>>();

    /** The first fence is the fence that comes before the first entry. */
    private firstFence = new LockedResource();

    public constructor(btree: BTreeComponent) {
        this.btree = btree;
    }

    private getContent(key: string): LockedResource {
        const out = this.content.get(key);
        if (out) { return out; }
        const newOut = new LockedResource(() => this.content.delete(key));
        this.content.set(key, newOut);
        return newOut;
    }

    private getFence(key?: string): LockedResource {
        if (!key) { return this.firstFence; }
        const out = this.fences.get(key);
        if (out) { return out; }
        const newOut = new LockedResource(() => this.fences.delete(key));
        this.fences.set(key, newOut);
        return newOut;
    }

    private getLocks(holder: LockHolder): LuaSet<Lock> {
        const out = this.locksPerTx.get(holder);
        if (out) { return out; }
        const newOut = new LuaSet<Lock>();
        this.locksPerTx.set(holder, newOut);
        return newOut;
    }

    /** Acquires an exclusive lock on a resource or upgrades if possible. */
    private exclusive(resource: LockedResource, holder: LockHolder): Lock {
        if (!resource.mode || !resource.holders.has(holder)) {
            const lock = Lock.exclusive(holder, resource);
            this.getLocks(holder).add(lock);
            return lock;
        } else {
            const lock = resource.holders.get(holder)!;
            lock.upgrade();
            return lock;
        }
    }

    /** Acquires a shared lock on a resource. */
    private shared(resource: LockedResource, holder: LockHolder): Lock {
        if (!resource.mode || !resource.holders.has(holder)) {
            const lock = Lock.shared(holder, resource);
            this.getLocks(holder).add(lock);
            return lock;
        }
        return resource.holders.get(holder)!;
    }

    /** Releases a lock and unlinks related structures. */
    private release(lock: Lock) {
        lock.release();
        this.getLocks(lock.holder).delete(lock);
    }

    /** Releases all locks on a transaction and unlinks related structures. */
    public releaseLocks(holder: LockHolder): void {
        for (const lock of this.getLocks(holder)) { lock.release(); }
        this.locksPerTx.delete(holder);
        os.queueEvent("lock_released");
    }

    private exclusiveFence(
        cl: TxCollection,
        key: string,
        holder: LockHolder,
        prev?: KvPair,
    ) {
        // Locking may change what the previous node is, so we need to keep
        // trying until it settles.
        let oldPrev = prev;
        let oldLock = this.exclusive(this.getFence(oldPrev?.key), holder);
        while (true) {
            const [newPrev] = this.btree.search(cl, key);
            if (oldPrev?.key == newPrev?.key) { break; }
            const newLock = this.exclusive(this.getFence(newPrev?.key), holder);
            this.release(oldLock);
            oldLock = newLock;
            oldPrev = newPrev;
        }
    }

    private sharedFence(
        cl: TxCollection,
        key: string,
        holder: LockHolder,
        prev?: KvPair,
    ) {
        // Locking may change what the previous node is, so we need to keep
        // trying until it settles.
        let oldPrev = prev;
        let oldLock = this.shared(this.getFence(oldPrev?.key), holder);
        while (true) {
            const [newPrev] = this.btree.search(cl, key);
            if (oldPrev?.key == newPrev?.key) { break; }
            const newLock = this.shared(this.getFence(newPrev?.key), holder);
            this.release(oldLock);
            oldLock = newLock;
            oldPrev = newPrev;
        }
    }

    /** Acquires locks for setting/inserting a value. */
    public acquireSet(cl: TxCollection, key: string, holder: LockHolder): void {
        this.exclusive(this.getContent(key), holder);

        const [prev, next] = this.btree.search(cl, key);
        if (!next || next.key != key) {
            // Key doesn't exist, insertion requires acquiring fence locks.
            this.exclusiveFence(cl, key, holder, prev);
        }
    }

    /** Acquires locks for deleting a value. */
    public acquireDelete(cl: TxCollection, key: string, holder: LockHolder): void {
        this.exclusive(this.getContent(key), holder);

        const [prev, next] = this.btree.search(cl, key);
        if (next && next.key == key) {
            // Key exists, deletion requires acquiring fence locks.
            this.exclusiveFence(cl, key, holder, prev);
        }
    }

    /** Acquires locks for getting a value. */
    public acquireGet(key: string, holder: LockHolder): void {
        this.shared(this.getContent(key), holder);
    }

    /** Acquires locks for getting the next key and value. */
    public acquireNext(cl: TxCollection, key: string, holder: LockHolder): void {
        const [prev, next] = this.btree.search(cl, key);
        if (!next || next.key != key) {
            // Key doesn't exist, we need to acquire fence locks.
            this.sharedFence(cl, key, holder, prev);

            // Now we can carry on the search and lock the content.
            const [_, cNext] = this.btree.search(cl, key);
            if (cNext) { this.shared(this.getContent(cNext.key), holder); }
        } else {
            // Key exists so lock the content.
            this.shared(this.getContent(key), holder);
        }
    }
}
