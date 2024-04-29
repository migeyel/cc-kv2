import { Deque, DequeNode } from "../Deque";

/** The set of all resources that holders are waiting for */
export const waitingFor = new LuaMap<LockHolder, LockedResource>();

/**
 * Finds and breaks deadlocks.
 * @returns A set of lock holders that must cancel to break the deadlock.
 */
export function breakDeadlocks(): LuaSet<LockHolder> {
    const out = new LuaSet<LockHolder>();
    const wfCopy = new LuaMap<LockHolder, LockedResource>();
    for (const [k, v] of waitingFor) { wfCopy.set(k, v); }

    const open = new LuaSet<LockHolder>();
    const closed = new LuaSet<LockHolder>();

    const dfs = (v: LockHolder) => {
        if (closed.has(v)) { return; }
        if (!wfCopy.has(v)) { return; }
        if (open.has(v)) {
            out.add(v);
            open.delete(v);
            wfCopy.delete(v);
            return;
        }

        open.add(v);
        for (const child of assert(wfCopy.get(v)).holders) {
            dfs(child);
            if (!wfCopy.has(v)) { return; }
        }
        closed.add(v);
    };

    const vertices = new LuaSet<LockHolder>();
    for (const [v] of wfCopy) { vertices.add(v); }
    for (const v of vertices) { dfs(v); }

    return out;
}


enum LockMode {
    EXCLUSIVE,
    SHARED,
}

export type LockCb = () => void;

type Ticket = {
    holder: LockHolder,
    mode: LockMode,
}

/** A resource accessible through locking. */
export class LockedResource {
    /** Locks waiting to acquire the resource. */
    public queue = new Deque<Ticket>();

    /** Holders waiting for the resource. */
    public waiting = new LuaMap<LockHolder, DequeNode<Ticket>>();

    /** Holders currently holding the lock in shared or exclusive mode. */
    public holders = new LuaSet<LockHolder>();

    /** A holder currently holding the lock in exclusive mode. */
    public exclusiveHolder?: LockHolder;

    /** Called when there are no holders left holding or waiting for the resource. */
    public onEmpty: () => void;

    public emptyCheck() {
        if (this.queue.isEmpty() && this.holders.isEmpty()) { this.onEmpty(); }
    }

    public constructor(onEmpty?: () => void) {
        this.onEmpty = onEmpty || (() => {});
    }
}

/** An actor that can hold a lock. */
export class LockHolder {
    /** Resources the holder is holding. */
    private held = new LuaSet<LockedResource>();

    /** The resource the holder is currently waiting on, if any. */
    private waiting?: LockedResource;

    /** Acquires a resource in exclusive mode. */
    public acquireExclusive(resource: LockedResource) {
        assert(!this.waiting);
        this.waiting = resource;
        waitingFor.set(this, resource);
        resource.queue.pushBack({ holder: this, mode: LockMode.EXCLUSIVE });
        while (!this.tryAcquire()) { coroutine.yield(); }
    }

    /** Acquires a resource in shared mode. */
    public acquireShared(resource: LockedResource) {
        assert(!this.waiting);
        this.waiting = resource;
        waitingFor.set(this, resource);
        resource.queue.pushBack({ holder: this, mode: LockMode.SHARED });
        while (!this.tryAcquire()) { coroutine.yield; }
    }

    /** Tries to acquire a waited for resource. */
    private tryAcquire(): boolean {
        const resource = assert(this.waiting);
        const ticket = assert(resource.queue.first()).val;
        if (assert(resource.queue.first()).val.holder == this) {
            // We reached the front of the queue.
            if (resource.exclusiveHolder != undefined) {
                // There is an exclusive holder.
                if (resource.exclusiveHolder == this) {
                    // We already hold the resource. No-op.
                    resource.queue.popFront();
                    this.waiting = undefined;
                    waitingFor.delete(this);
                    return true;
                } else {
                    // We have to wait for the holder to release.
                    return false;
                }
            } else if (!resource.holders.isEmpty()) {
                // There are only shared holders.
                if (ticket.mode == LockMode.SHARED) {
                    // Share the resource with them.
                    resource.queue.popFront();
                    this.waiting = undefined;
                    waitingFor.delete(this);
                    this.held.add(resource);
                    resource.holders.add(this);
                    return true;
                } else {
                    const [first] = next(resource.holders);
                    const [second] = next(resource.holders, first);
                    if (first == this && second == undefined) {
                        // We're the sole shared holder. Upgrade the lock.
                        resource.queue.popFront();
                        this.waiting = undefined;
                        waitingFor.delete(this);
                        resource.exclusiveHolder = this;
                        return true;
                    } else {
                        // We have to wait for the other holders to release.
                        return false;
                    }
                }
            } else {
                // There are no holders.
                if (ticket.mode == LockMode.EXCLUSIVE) {
                    // Acquire in exclusive mode.
                    resource.queue.popFront();
                    this.waiting = undefined;
                    waitingFor.delete(this);
                    this.held.add(resource);
                    resource.exclusiveHolder = this;
                    resource.holders.add(this);
                    return true;
                } else {
                    // Acquire in shared mode.
                    resource.queue.popFront();
                    this.waiting = undefined;
                    waitingFor.delete(this);
                    this.held.add(resource);
                    resource.holders.add(this);
                    return true;
                }
            }
        } else {
            // We haven't reached the front of the queue.
            return false;
        }
    }

    /** Cancels waiting for a resource. */
    public abort() {
        const resource = assert(this.waiting);
        assert(resource.waiting.get(this)).pop();
        this.waiting = undefined;
        waitingFor.delete(this);
        resource.emptyCheck();
    }

    /** Releases a held resource. */
    public release(resource: LockedResource) {
        assert(this.held.has(resource));
        assert(this.waiting != resource);
        this.held.delete(resource);
        resource.exclusiveHolder = undefined;
        resource.holders.delete(this);
        resource.emptyCheck();
    }

    /**
     * Releases all resources held and being awaited.
     * @returns All resources held before releasing.
     */
    public releaseAll(): LuaSet<LockedResource> {
        const out = new LuaSet<LockedResource>();

        if (this.waiting) {
            out.add(this.waiting);
            this.abort();
        }

        for (const h of this.held) {
            out.add(h);
            this.release(h);
        }

        return out;
    }
}
