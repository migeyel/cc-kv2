import { WeakQueue } from "../WeakQueue";
import { uid } from "../uid";

const LOCK_RELEASED = "lock_released";

export class LockedResource {
    public id = uid();
    public queue = new WeakQueue<Ticket>();
    public refCount = 0;
    public mode?: LockMode;
}

/** A held lock on a resource. */
export class Lock {
    /** The shared resource, which includes a slot and a queue. */
    private resource: LockedResource;

    /** Whether this lock is being held or has been released. */
    private held = true;

    private constructor(resource: LockedResource) {
        this.resource = resource;
    }

    public static exclusive(resource: LockedResource): Lock {
        // If the slot is free, take it.
        if (!resource.mode) {
            resource.mode = LockMode.EXCLUSIVE;
            resource.refCount++;
            return new Lock(resource);
        }

        // Enter the queue.
        const ownTicket = { mode: LockMode.EXCLUSIVE };
        resource.queue.enqueue(ownTicket);
        while (true) {
            if (resource.queue.peek() == ownTicket && !resource.mode) {
                // We've reached the front, and there's no lock in the slot.
                resource.queue.dequeue();
                resource.mode = LockMode.EXCLUSIVE;
                resource.refCount++;
                return new Lock(resource);
            }
            os.pullEvent(LOCK_RELEASED);
        }
    }

    public static shared(resource: LockedResource): Lock {
        // If the slot is free, take it.
        if (!resource.mode) {
            resource.mode = LockMode.SHARED;
            resource.refCount++;
            return new Lock(resource);
        }

        // Enter the queue.
        const ownTicket = { mode: LockMode.SHARED };
        resource.queue.enqueue(ownTicket);
        while (true) {
            if (resource.queue.peek() == ownTicket) {
                // We've reached the front.
                if (!resource.mode) {
                    // There are no locks on the resource.
                    resource.queue.dequeue();
                    resource.mode = LockMode.SHARED;
                    resource.refCount++;
                    return new Lock(resource);
                } else if (resource.mode == LockMode.SHARED) {
                    // There are shared locks on the resource.
                    resource.queue.dequeue();
                    resource.refCount++;
                    return new Lock(resource);
                }
            }
            os.pullEvent(LOCK_RELEASED);
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
        const ticket = { mode: LockMode.EXCLUSIVE };
        this.resource.queue.enqueue(ticket);
        while (true) {
            const front = this.resource.queue.peek();
            if (
                front &&
                front.mode == LockMode.EXCLUSIVE &&
                this.resource.refCount == 1
            ) {
                // The front is an exclusive intent and we're the sole lock
                // holder. Upgrading right now is as fair as it can be.
                this.resource.mode = LockMode.EXCLUSIVE;
                return;
            }
            os.pullEvent(LOCK_RELEASED);
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
        if (--this.resource.refCount == 0) { this.resource.mode = undefined; }
    }
}

/** A ticket on a queue waiting for a some lock to be released. */
type Ticket = { mode: LockMode };

enum LockMode {
    EXCLUSIVE,
    SHARED,
}
