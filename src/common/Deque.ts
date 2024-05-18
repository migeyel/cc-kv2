class DequeHead<T> {
    public tag = 0 as const;
    public next: DequeNode<T> | DequeTail<T>;

    public pushAfter(value: T): DequeNode<T> {
        const node = new DequeNode(value, this, this.next);
        this.next.prev = node;
        this.next = node;
        return node;
    }

    public constructor(next: DequeNode<T> | DequeTail<T>) {
        this.next = next;
    }
}

class DequeTail<T> {
    public tag = 1 as const;
    public prev: DequeNode<T> | DequeHead<T>;

    public pushBefore(value: T): DequeNode<T> {
        const node = new DequeNode(value, this.prev, this);
        this.prev.next = node;
        this.prev = node;
        return node;
    }

    public constructor(prev: DequeNode<T> | DequeHead<T>) {
        this.prev = prev;
    }
}

const POPPED_RDERR = new Error("attempt to read link of popped queue node");
const POPPED_WRERR = new Error("attempt to write to link of popped queue node");

export class DequeNode<T> {
    public tag = 2 as const;
    public val: T;
    public prev: DequeNode<T> | DequeHead<T>;
    public next: DequeNode<T> | DequeTail<T>;
    public popped = false;

    public getPrev(): DequeNode<T> | undefined {
        if (this.popped) { throw POPPED_RDERR; }
        const out = this.prev;
        if (out.tag == 2) { return out; }
    }

    public getNext(): DequeNode<T> | undefined {
        if (this.popped) { throw POPPED_RDERR; }
        const out = this.next;
        if (out.tag == 2) { return out; }
    }

    public pop(): T {
        if (this.popped) { return this.val; }
        this.prev.next = this.next;
        this.next.prev = this.prev;
        this.popped = true;
        return this.val;
    }

    public pushBefore(value: T): DequeNode<T> {
        if (this.popped) { throw POPPED_WRERR; }
        const node = new DequeNode(value, this.prev, this);
        this.prev.next = node;
        this.prev = node;
        return node;
    }

    public pushAfter(value: T): DequeNode<T> {
        if (this.popped) { throw POPPED_WRERR; }
        const node = new DequeNode(value, this, this.next);
        this.next.prev = node;
        this.next = node;
        return node;
    }

    public constructor(
        value: T,
        prev: DequeNode<T> | DequeHead<T>,
        next: DequeNode<T> | DequeTail<T>,
    ) {
        this.val = value;
        this.prev = prev;
        this.next = next;
    }
}

export class Deque<T> {
    public head: DequeHead<T>;
    public tail: DequeTail<T>;

    public isEmpty(): boolean {
        return this.head.next == this.tail;
    }

    public first(): DequeNode<T> | undefined {
        const next = this.head.next;
        if (next.tag == 2) { return next; }
    }

    public last(): DequeNode<T> | undefined {
        const prev = this.tail.prev;
        if (prev.tag == 2) { return prev; }
    }

    public pushFront(val: T): DequeNode<T> {
        return this.head.pushAfter(val);
    }

    public pushBack(val: T): DequeNode<T> {
        return this.tail.pushBefore(val);
    }

    public popFront(): T | undefined {
        const out = this.first();
        if (out) { return out.pop(); }
    }

    public popBack(): T | undefined {
        const out = this.last();
        if (out) { return out.pop(); }
    }

    public constructor() {
        // Hack :^)
        this.head = new DequeHead(undefined as unknown as DequeTail<T>);
        this.tail = new DequeTail(this.head);
        this.head.next = this.tail;
    }
}
