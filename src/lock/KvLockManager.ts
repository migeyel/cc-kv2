import { BTreeComponent, KvPair } from "../btree/Node";
import { TxCollection } from "../txStore/LogStore";
import { LockHolder, LockedResource } from "./Lock";

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

    private exclusiveFence(
        cl: TxCollection,
        key: string,
        holder: LockHolder,
        prev?: KvPair,
    ) {
        // Locking may change what the previous node is, so we need to keep
        // trying until it settles.
        let oldPrev = prev;
        holder.acquireExclusive(this.getFence(oldPrev?.key));
        while (true) {
            const [newPrev] = this.btree.search(cl, key);
            if (oldPrev?.key == newPrev?.key) { break; }
            holder.acquireExclusive(this.getFence(newPrev?.key));
            holder.release(this.getFence(oldPrev?.key));
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
        holder.acquireShared(this.getFence(oldPrev?.key));
        while (true) {
            const [newPrev] = this.btree.search(cl, key);
            if (oldPrev?.key == newPrev?.key) { break; }
            holder.acquireShared(this.getFence(newPrev?.key));
            holder.release(this.getFence(oldPrev?.key));
            oldPrev = newPrev;
        }
    }

    /** Acquires locks for setting/inserting a value. */
    public acquireSet(cl: TxCollection, key: string, holder: LockHolder): void {
        holder.acquireExclusive(this.getContent(key));

        const [prev, next] = this.btree.search(cl, key);
        if (!next || next.key != key) {
            // Key doesn't exist, insertion requires acquiring fence locks.
            this.exclusiveFence(cl, key, holder, prev);
        }
    }

    /** Acquires locks for deleting a value. */
    public acquireDelete(cl: TxCollection, key: string, holder: LockHolder): void {
        holder.acquireExclusive(this.getContent(key));

        const [prev, next] = this.btree.search(cl, key);
        if (next && next.key == key) {
            // Key exists, deletion requires acquiring fence locks.
            this.exclusiveFence(cl, key, holder, prev);
        }
    }

    /** Acquires locks for getting a value. */
    public acquireGet(key: string, holder: LockHolder): void {
        holder.acquireShared(this.getContent(key));
    }

    /** Acquires locks for getting the next key and value. */
    public acquireNext(cl: TxCollection, key: string, holder: LockHolder): void {
        const [prev, next] = this.btree.search(cl, key);
        if (!next || next.key != key) {
            // Key doesn't exist, we need to acquire fence locks.
            this.sharedFence(cl, key, holder, prev);

            // Now we can carry on the search and lock the content.
            const [_, cNext] = this.btree.search(cl, key);
            if (cNext) { holder.acquireShared(this.getContent(cNext.key)); }
        } else {
            // Key exists so lock the content.
            holder.acquireShared(this.getContent(key));
        }
    }
}
