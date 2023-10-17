/**
 * A weak queue.
 *
 * This structure has the same semantics as a table with __mode "v", except that
 * its insertions and deletions follow the FIFO order.
 */
export class WeakQueue<T extends object> {
    private entries = setmetatable(
        new LuaMap<number, T>(),
        { __mode: "v" },
    );

    private front = 1;
    private back = 1;

    public isEmpty(): boolean {
        return !next(this.entries)[0];
    }

    public enqueue(value: T) {
        this.entries.set(this.back++, value);
    }

    public peek(): T | undefined {
        if (this.isEmpty()) { return; }
        while (this.front < this.back) {
            const out = this.entries.get(this.front);
            if (out) { return out; }
            this.front++;
        }
    }

    public dequeue(): T | undefined {
        if (this.isEmpty()) { return; }
        while (this.front < this.back) {
            const out = this.entries.get(this.front);
            this.entries.delete(this.front++);
            if (out) { return out; }
        }
    }
}
