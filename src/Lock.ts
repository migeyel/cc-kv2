export class LockedResource {
    public queue = new TicketQueue();
    public slot?: Lock;
}

/** A held lock on a resource. */
export class Lock {
    private mode: LockMode;

    /** The shared resource, which includes a slot and a queue. */
    private resource: LockedResource;

    /** Number of times this lock has been locked, always 1 for exclusive. */
    private refCount = 1;

    /** Whether there's a thread wishing to upgrade this lock. */
    private isUpgrading = false;

    private constructor(resource: LockedResource, mode: LockMode) {
        this.resource = resource;
        this.mode = mode;
    }

    public static exclusive(resource: LockedResource): Lock {
        const ownLock = new Lock(resource, LockMode.EXCLUSIVE);

        // If the slot is free, take it.
        if (!resource.slot) {
            resource.slot = ownLock;
            return ownLock;
        }

        // Enter the queue.
        const ownTicket = resource.queue.enqueue(LockMode.EXCLUSIVE);
        while (true) {
            os.pullEvent("lock_released");
            if (resource.queue.peek() == ownTicket && !resource.slot) {
                // We've reached the front, and there's no lock in the slot.
                resource.queue.dequeue();
                resource.slot = ownLock;
                return ownLock;
            }
        }
    }

    public static shared(resource: LockedResource): Lock {
        const ownLock = new Lock(resource, LockMode.SHARED);

        // If the slot is free, take it.
        const held = resource.slot;
        if (held == undefined) {
            resource.slot = ownLock;
            return ownLock;
        }

        // Enter the queue.
        const ownTicket = resource.queue.enqueue(LockMode.SHARED);
        while (true) {
            os.pullEvent("lock_released");
            if (resource.queue.peek() == ownTicket) {
                // We've reached the front.
                const held2 = resource.slot;
                if (held2 == undefined) {
                    // There's no lock in the slot.
                    resource.queue.dequeue();
                    resource.slot = ownLock;
                    return ownLock;
                } else if (held2.mode == LockMode.SHARED) {
                    // There's another shared lock in the slot.
                    resource.queue.dequeue();
                    held2.refCount += 1;
                    return held2;
                }
            }
        }
    }

    /**
     * Checks if this lock is being held.
     * @returns Whether this lock is currently held and can be interacted with.
     */
    public isHeld(): boolean {
        return this.resource.slot == this;
    }

    /**
     * Checks if this lock is shared.
     * @returns Whether this lock is a shared lock.
     * @throws If this lock has been released.
     */
    public isShared(): boolean {
        assert(this.isHeld(), "attempt to interact with a non-held lock");
        return this.mode == LockMode.SHARED;
    }

    /**
     * Tries to upgrade from shared to exclusive. No-op on exclusive locks.
     * @returns Whether the upgrade succeeded or failed in deadlock.
     * @throws If this lock has been released.
     */
    public tryUpgrade(): boolean {
        assert(this.isHeld(), "attempt to interact with a non-held lock");
        if (!this.isShared()) { return true; }

        // Declare our intent to upgrade.
        if (this.isUpgrading) { return false; }
        this.isUpgrading = true;

        // Enter the queue with an exclusive intent.
        const _ticket = this.resource.queue.enqueue(LockMode.EXCLUSIVE);
        while (true) {
            const front = this.resource.queue.peek();
            if (
                front &&
                front.mode == LockMode.EXCLUSIVE &&
                this.refCount == 1
            ) {
                // The front is an exclusive intent and we're the sole lock
                // holder. Upgrading right now is as fair as it can be.
                this.mode = LockMode.EXCLUSIVE;
                this.isUpgrading = false;
                return true;
            }
            os.pullEvent("lock_released");
        }
    }

    /**
     * Downgrades from exclusive to shared. No-op on shared locks.
     * @throws If this lock has been released.
     */
    public downgrade() {
        assert(this.isHeld(), "attempt to interact with a non-held lock");
        if (this.isShared()) { return; }
        this.mode = LockMode.SHARED;
        os.queueEvent("lock_released");
    }

    /**
     * Releases the lock, freeing the resource for other thread usage.
     *
     * After this action, any other method calls will throw, except for isHeld.
     */
    public release() {
        assert(this.isHeld(), "attempt to interact with a non-held lock");
        if (this.refCount-- == 0) { this.resource.slot = undefined; }
        os.queueEvent("lock_released");
    }
}

/** A queue of tickets held by threads waiting for a lock to be released. */
class TicketQueue {
    // This queue is specialized in letting ticket holders drop their tickets.
    // This is necessary because a thread can be cancelled while waiting on a
    // lock, which causes its ticket to be dropped when the thread is dropped.
    private entries = setmetatable(
        new LuaMap<number, Ticket>(),
        { __mode: "v" },
    );

    private front = 1;
    private back = 1;

    public isEmpty(): boolean {
        return !next(this.entries)[0];
    }

    public enqueue(mode: LockMode): Ticket {
        const ticket = { mode };
        this.entries.set(this.back, ticket);
        this.back += 1;
        return ticket;
    }

    public peek(): Ticket | undefined {
        if (this.isEmpty()) { return; }
        while (this.front < this.back) {
            const out = this.entries.get(this.front);
            if (out) { return out; }
            this.front += 1;
        }
    }

    public dequeue(): Ticket | undefined {
        if (this.isEmpty()) { return; }
        while (this.front < this.back) {
            const out = this.entries.get(this.front);
            this.entries.delete(this.front);
            this.front += 1;
            if (out) {
                return out;
            }
        }
    }
}

/** A ticket on a queue waiting for a some lock to be released. */
type Ticket = { mode: LockMode };

enum LockMode {
    EXCLUSIVE,
    SHARED,
}
