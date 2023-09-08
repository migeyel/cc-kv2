import { ShMap } from "../ShMap";
import {
    IPage,
    IPageStore,
    IStoreCollection,
    MAX_NAMESPACE,
    Namespace,
    PageNum,
    PageSize,
} from "./IPageStore";

/** An in-memory store impplementation. */
export class MemCollection implements IStoreCollection<MemPage, MemStore> {
    public readonly pageSize: PageSize;

    private state = new MemState();

    private stores = new ShMap<Namespace, MemStore>();

    public constructor(pageSize: PageSize) {
        this.pageSize = pageSize;
    }

    public getStore(namespace: Namespace): MemStore {
        assert(namespace <= MAX_NAMESPACE);
        return this.stores.getOr(this, namespace, () => new MemStore(
            this.pageSize,
            namespace,
            this.state,
        ));
    }

    public listStores(): LuaSet<Namespace> {
        const out = new LuaSet<Namespace>();
        for (const [store] of this.state.stores) {
            out.add(store as Namespace);
        }
        return out;
    }
}

class MemStore implements IPageStore<MemPage> {
    public readonly pageSize: PageSize;

    public readonly namespace: Namespace;

    private state: MemState;

    private pages = new ShMap<PageNum, MemPage>();

    public constructor(
        pageSize: PageSize,
        namespace: Namespace,
        state: MemState,
    ) {
        this.pageSize = pageSize;
        this.namespace = namespace;
        this.state = state;
    }

    public getPage(pageNum: PageNum): MemPage {
        return this.pages.getOr(this, pageNum, () => new MemPage(
            this.pageSize,
            pageNum,
            this.namespace,
            this.state,
        ));
    }

    public listPages(): LuaSet<PageNum> {
        const out = new LuaSet<PageNum>();
        const store = this.state.stores.get(this.namespace);
        if (store) { for (const [page] of store) { out.add(page); } }
        return out;
    }
}

class MemPage implements IPage {
    public readonly pageSize: PageSize;

    public readonly pageNum: PageNum;

    public readonly namespace: Namespace;

    private state: MemState;

    private isAppend = false;

    public constructor(
        pageSize: PageSize,
        pageNum: PageNum,
        namespace: Namespace,
        state: MemState,
    ) {
        this.pageSize = pageSize;
        this.pageNum = pageNum;
        this.namespace = namespace;
        this.state = state;
    }

    public exists(): boolean {
        return this.read() != undefined;
    }

    public create(initialData?: string): void {
        assert(!this.isAppend);
        this.state.setPage(this.namespace, this.pageNum, initialData || "");
    }

    public createOpen(): void {
        assert(!this.isAppend);
        this.create();
    }

    public delete(): void {
        if (this.isAppend) { this.closeAppend(); }
        this.state.delPage(this.namespace, this.pageNum);
    }

    public read(): string | undefined {
        return this.state.getPage(this.namespace, this.pageNum);
    }

    public write(data: string): void {
        assert(!this.isAppend);
        this.state.setPage(this.namespace, this.pageNum, data);
    }

    public append(extra: string): void {
        assert(this.isAppend);
        this.write(this.read() + extra);
    }

    public canAppend(): boolean {
        return this.isAppend;
    }

    public openAppend(): void {
        assert(!this.isAppend);
        this.isAppend = true;
    }

    public closeAppend(): void {
        assert(this.isAppend);
        this.isAppend = false;
    }

    public flush(): void { }
}

class MemState {
    public stores = new LuaMap<Namespace, LuaMap<PageNum, string>>();

    public setPage(namespace: Namespace, pageNum: PageNum, data: string) {
        let store = this.stores.get(namespace);
        if (!store) {
            store = new LuaMap();
            this.stores.set(namespace, store);
        }
        store.set(pageNum, data);
    }

    public getPage(namespace: Namespace, pageNum: PageNum): string | undefined {
        const store = this.stores.get(namespace);
        if (!store) { return; }
        return store.get(pageNum);
    }

    public delPage(namespace: Namespace, pageNum: PageNum) {
        const store = this.stores.get(namespace);
        if (!store) { return; }
        store.delete(pageNum);
        if (!next(store)[0]) { this.stores.delete(namespace); }
    }
}
