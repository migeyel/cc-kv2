import { WeakQueue } from "../WeakQueue";
import { uid } from "../uid";

const LOCK_RELEASED = "lock_released";
const DEADLOCK_VICTIM = "deadlock_victim";

export const waitForGraph = new LuaMap<LockHolder, LockedResource>();

export function breakDeadlocks() {
    const open = new LuaSet<LockHolder>();
    const closed = new LuaSet<LockHolder>();

    const dfs = (v: LockHolder) => {
        if (closed.has(v)) { return; }
        if (!waitForGraph.has(v)) { return; }
        if (open.has(v)) {
            os.queueEvent(DEADLOCK_VICTIM, v.id);
            open.delete(v);
            waitForGraph.delete(v);
            return;
        }

        open.add(v);
        for (const [child] of waitForGraph.get(v)!.holders) {
            dfs(child);
            if (!waitForGraph.has(v)) { return; }
        }
        closed.add(v);
    };

    const vertices = new LuaSet<LockHolder>();
    for (const [v] of waitForGraph) { vertices.add(v); }
    for (const v of vertices) { dfs(v); }
}

export class LockedResource {
    public id = uid();
    public queue = new WeakQueue<Ticket>();
    public holders = new LuaMap<LockHolder, Lock>();
    public mode?: LockMode;
}

export class LockHolder {
    public readonly id = uid();
}

/** A held lock on a resource. */
export class Lock {
    /** The shared resource, which includes a slot and a queue. */
    public readonly resource: LockedResource;

    /** The lock's holder. */
    public readonly holder: LockHolder;

    /** Whether this lock is being held or has been released. */
    private held = true;

    private constructor(holder: LockHolder, resource: LockedResource) {
        this.holder = holder;
        this.resource = resource;
    }

    public static exclusive(holder: LockHolder, resource: LockedResource): Lock {
        const lock = new Lock(holder, resource);

        // If the slot is free, take it.
        if (!resource.mode) {
            resource.mode = LockMode.EXCLUSIVE;
            resource.holders.set(holder, lock);
            return lock;
        }

        // Enter the queue.
        waitForGraph.set(holder, resource);
        const ownTicket = { mode: LockMode.EXCLUSIVE };
        resource.queue.enqueue(ownTicket);
        while (true) {
            if (resource.queue.peek() == ownTicket && !resource.mode) {
                // We've reached the front, and there's no lock in the slot.
                resource.queue.dequeue();
                resource.mode = LockMode.EXCLUSIVE;
                resource.holders.set(holder, lock);
                waitForGraph.delete(holder);
                return lock;
            }

            while (true) {
                const ev = os.pullEvent();
                if (ev[0] == LOCK_RELEASED) {
                    break;
                } else if (ev[0] == DEADLOCK_VICTIM && ev[1] == holder.id) {
                    throw "deadlock";
                }
            }
        }
    }

    public static shared(holder: LockHolder, resource: LockedResource): Lock {
        const lock = new Lock(holder, resource);

        // If the slot is free, take it.
        if (!resource.mode) {
            resource.mode = LockMode.SHARED;
            resource.holders.set(holder, lock);
            return lock;
        }

        // Enter the queue.
        waitForGraph.set(holder, resource);
        const ownTicket = { mode: LockMode.SHARED };
        resource.queue.enqueue(ownTicket);
        while (true) {
            if (resource.queue.peek() == ownTicket) {
                // We've reached the front.
                if (!resource.mode) {
                    // There are no locks on the resource.
                    resource.queue.dequeue();
                    resource.mode = LockMode.SHARED;
                    resource.holders.set(holder, lock);
                    waitForGraph.delete(holder);
                    return lock;
                } else if (resource.mode == LockMode.SHARED) {
                    // There are shared locks on the resource.
                    resource.queue.dequeue();
                    resource.holders.set(holder, lock);
                    waitForGraph.delete(holder);
                    return lock;
                }
            }

            while (true) {
                const ev = os.pullEvent();
                if (ev[0] == LOCK_RELEASED) {
                    break;
                } else if (ev[0] == DEADLOCK_VICTIM && ev[1] == holder.id) {
                    throw "deadlock";
                }
            }
        }
    }

    /**
     * Checks if this lock is being held.
     * @returns Whether this lock is currently held and can be interacted with.
     */
    public isHeld(): boolean {
        return this.held;
    }

    /**
     * Checks if this lock is shared.
     * @returns Whether this lock is a shared lock.
     * @throws If this lock has been released.
     */
    public isShared(): boolean {
        assert(this.isHeld(), "attempt to interact with a non-held lock");
        return this.resource.mode == LockMode.SHARED;
    }

    /**
     * Tries to upgrade from shared to exclusive. No-op on exclusive locks.
     * @throws If this lock has been released.
     */
    public upgrade(): void {
        assert(this.isHeld(), "attempt to interact with a non-held lock");
        if (!this.isShared()) { return; }

        // Enter the queue with an exclusive intent.
        waitForGraph.set(this.holder, this.resource);
        const ticket = { mode: LockMode.EXCLUSIVE };
        this.resource.queue.enqueue(ticket);
        while (true) {
            const front = this.resource.queue.peek()!;
            if (
                front.mode == LockMode.EXCLUSIVE &&
                next(this.resource.holders)[0] == this.holder &&
                next(this.resource.holders, this.holder)[0] == undefined
            ) {
                // The front is an exclusive intent and we're the sole lock
                // holder. Upgrading right now is as fair as it can be.
                this.resource.mode = LockMode.EXCLUSIVE;
                waitForGraph.delete(this.holder);
                return;
            }

            while (true) {
                const ev = os.pullEvent();
                if (ev[0] == LOCK_RELEASED) {
                    break;
                } else if (ev[0] == DEADLOCK_VICTIM && ev[1] == this.holder.id) {
                    throw "deadlock";
                }
            }
        }
    }

    /**
     * Downgrades from exclusive to shared. No-op on shared locks.
     * @throws If this lock has been released.
     */
    public downgrade() {
        assert(this.isHeld(), "attempt to interact with a non-held lock");
        if (this.isShared()) { return; }
        this.resource.mode = LockMode.SHARED;
    }

    /**
     * Releases the lock, freeing the resource for other thread usage.
     *
     * After this action, any other method calls will throw, except for isHeld.
     */
    public release() {
        assert(this.isHeld(), "attempt to interact with a non-held lock");
        this.held = false;
        this.resource.holders.delete(this.holder);
        if (this.resource.holders.isEmpty()) { this.resource.mode = undefined; }
    }
}

/** A ticket on a queue waiting for a some lock to be released. */
type Ticket = { mode: LockMode };

enum LockMode {
    EXCLUSIVE,
    SHARED,
}
