export class Task {
    private dead = false;

    public wasKilled(): boolean {
        return this.dead;
    }

    public kill() {
        this.dead = true;
    }
}

export class Scheduler {
    private unfiltered = new LuaSet<LuaThread>();
    private filtered = new LuaMap<string, LuaSet<LuaThread>>();

    private tasks = setmetatable(
        new LuaMap<LuaThread, Task>(),
        { __mode: "k" },
    );

    private runOnce(thread: LuaThread, ...args: any[]) {
        const task = this.tasks.get(thread);
        if (!task || task.wasKilled()) { return; }
        const yielded = coroutine.resume(thread, ...args);
        assert(yielded[0], yielded[1]);
        if (coroutine.status(thread) == "dead") { return; }
        if (yielded[1]) {
            let set = this.filtered.get(yielded[1]);
            if (!set) {
                set = new LuaSet();
                this.filtered.set(yielded[1], set);
            }
            set.add(thread);
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
            const filtered = this.filtered.get(event[0]);
            const unfiltered = this.unfiltered;
            this.filtered.delete(event[0]);
            this.unfiltered = new LuaSet();

            if (filtered) {
                for (const coro of filtered) {
                    this.runOnce(coro, ...event);
                }
            }

            for (const coro of unfiltered) {
                this.runOnce(coro, ...event);
            }
        }
    }
}
