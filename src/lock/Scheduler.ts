import { TxId } from "../txStore/LogStore";

export class Task {
    private dead = false;

    public wasKilled(): boolean {
        return this.dead;
    }

    public kill() {
        this.dead = true;
    }
}

const TX_REQUEST = "cckv2_request";
const LOCK_RELEASE = "lock_released";

export class Scheduler {
    private unfiltered = new LuaSet<LuaThread>();
    private filtered = new LuaMap<string, LuaSet<LuaThread>>();
    private lockReleases = new LuaMap<string, LuaSet<LuaThread>>();
    private txRequests = new LuaMap<TxId, LuaSet<LuaThread>>();

    private tasks = setmetatable(
        new LuaMap<LuaThread, Task>(),
        { __mode: "k" },
    );

    private getSetOrEmpty<T extends AnyNotNil, U extends AnyNotNil>(
        parent: LuaMap<T, LuaSet<U>>,
        key: T,
    ): LuaSet<U> {
        let out = parent.get(key);
        if (!out) {
            out = new LuaSet<U>();
            parent.set(key, out);
        }
        return out;
    }

    private runOnce(thread: LuaThread, ...args: any[]) {
        const task = this.tasks.get(thread);
        if (!task || task.wasKilled()) { return; }
        const yielded = coroutine.resume(thread, ...args);
        assert(yielded[0], yielded[1]);
        if (coroutine.status(thread) == "dead") { return; }
        if (yielded[1]) {
            if (yielded[1] == LOCK_RELEASE) {
                this.getSetOrEmpty(this.lockReleases, yielded[2]).add(thread);
            } else if (yielded[1] == TX_REQUEST) {
                this.getSetOrEmpty(this.txRequests, yielded[2]).add(thread);
            } else {
                this.getSetOrEmpty(this.filtered, yielded[1]).add(thread);
            }
        } else {
            this.unfiltered.add(thread);
        }
    }

    public add(fn: (this: void) => void, ...args: any[]): Task {
        const task = new Task();
        const thread = coroutine.create(fn);
        this.tasks.set(thread, task);
        this.runOnce(thread, args);
        return task;
    }

    public run() {
        while (true) {
            const event = os.pullEvent();

            const unfiltered = this.unfiltered;
            this.unfiltered = new LuaSet();

            const filtered = this.filtered.get(event[0]);
            this.filtered.delete(event[0]);

            let lockRelease;
            if (event[0] == LOCK_RELEASE) {
                lockRelease = this.lockReleases.get(event[1]);
                this.lockReleases.delete(event[1]);
            }

            let txRequest;
            if (event[0] == TX_REQUEST) {
                txRequest = this.txRequests.get(event[1]);
                this.txRequests.delete(event[1]);
            }

            if (filtered) {
                for (const coro of filtered) {
                    this.runOnce(coro, ...event);
                }
            }

            if (lockRelease) {
                for (const coro of lockRelease) {
                    this.runOnce(coro, ...event);
                }
            }

            if (txRequest) {
                for (const coro of txRequest) {
                    this.runOnce(coro, ...event);
                }
            }

            for (const coro of unfiltered) {
                this.runOnce(coro, ...event);
            }
        }
    }
}
