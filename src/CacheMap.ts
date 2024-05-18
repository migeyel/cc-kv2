import { Deque, DequeNode } from "./common/Deque";

export interface ICacheable {
    /** Performs side-effects of this object being evicted from the cache. */
    evict(): void;
}

/**
 * A shared cache quota and eviction queue. Entries in the cache hold a "ticket"
 * in this queue so that the LRU policy can be implemented generically.
 */
class CacheQueue {
    public maxQueueSize: number;

    public queue = new Deque<Ticket>();
    public queueMap = new LuaMap<Ticket, DequeNode<Ticket>>();
    public queueSize = 0;

    public pinned = new LuaSet<Ticket>();

    /** Creates a new ticket for a given ticket holder. */
    public enqueue(holder: ITicketHolder): Ticket {
        const ticket = new Ticket(holder);
        this.bump(ticket);
        return ticket;
    }

    /**
     * Puts a ticket in the back of the queue. If the queue is full and the
     * ticket isn't in it, evicts some other ticket. Does nothing for pinned
     * tickets.
     */
    public bump(ticket: Ticket) {
        if (this.pinned.has(ticket)) { return; }
        const node = this.queueMap.get(ticket);
        if (node) {
            node.pop();
        } else if (this.queueSize >= this.maxQueueSize) {
            const evictNode = this.queue.first()!;
            const evictTicket = evictNode.pop();
            this.queueMap.delete(evictTicket);
            this.queueSize--;
            evictTicket.evict();
        }
        this.queueMap.set(ticket, this.queue.pushBack(ticket));
        this.queueSize++;
    }

    /**
     * Pins a ticket, making sure it won't get evicted as long as it stays
     * pinned.
     */
    public pin(ticket: Ticket) {
        const node = this.queueMap.get(ticket);
        if (node) {
            this.queueMap.delete(node.pop());
            this.queueSize--;
        }
        this.pinned.add(ticket);
    }

    /** Unpins a ticket. */
    public unpin(ticket: Ticket) {
        this.pinned.delete(ticket);
        this.bump(ticket);
    }

    public constructor(maxSize: number) {
        this.maxQueueSize = maxSize;
    }
}

interface ITicketHolder {
    evict(ticket: Ticket): void;
}

class Ticket {
    private holder: ITicketHolder;

    public constructor(holder: ITicketHolder) {
        this.holder = holder;
    }

    public evict() {
        this.holder.evict(this);
    }
}

/** A LRU cache for objects. */
export class CacheMap<
    K extends AnyNotNil,
    V extends ICacheable
> implements ITicketHolder {
    private cache = new LuaMap<K, V>();
    private keys = new LuaMap<Ticket, K>();
    private tickets = new LuaMap<K, Ticket>();
    private queue: CacheQueue;

    public constructor(maxSize: number) {
        assert(maxSize > 0);
        this.queue = new CacheQueue(maxSize);
    }

    /** Internal use method. */
    public evict(ticket: Ticket): void {
        const key = assert(this.keys.get(ticket));
        assert(this.cache.get(key)).evict();
        this.cache.delete(key);
    }

    /** Creates a new cache that shares the quota and queue with this one. */
    public fork<L extends AnyNotNil, W extends ICacheable>(): CacheMap<L, W> {
        const out = new CacheMap<L, W>(1);
        out.queue = this.queue;
        return out;
    }

    /** Pins an entry if it is in the cache. */
    public pin(key: K) {
        this.queue.pin(assert(this.tickets.get(key)));
    }

    /** Unpins an entry if it is in the cache. */
    public unpin(key: K) {
        this.queue.unpin(assert(this.tickets.get(key)));
    }

    /** Gets a value from the cache or computes it from a closure. */
    public getOr(key: K, fn: () => V): V {
        const out = this.cache.get(key);
        if (out) { return out; }

        const newVal = fn();
        const ticket = this.queue.enqueue(this);
        this.keys.set(ticket, key);
        this.tickets.set(key, ticket);
        this.cache.set(key, newVal);

        return newVal;
    }
}
