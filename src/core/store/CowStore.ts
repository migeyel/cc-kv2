import { ShMap } from "../ShMap";
import {
    IPage,
    IPageStore,
    IStoreCollection,
    Namespace,
    PageNum,
    PageSize,
} from "./IPageStore";

class CowState {
    /** The collection containing the CoW write pages. Nil when not sharing. */
    public ref?: IStoreCollection<IPage, IPageStore<IPage>>;

    // We map one origin page to a pair of ref pages: one "regular" page, and one
    // deletion marker. We store this data in the page number to make the listPages impl
    // simpler.
    public getRef(origin: CowPage): { reg: IPage, del: IPage } | undefined {
        if (!this.ref) { return; }
        const store = this.ref.getStore(origin.namespace);
        return {
            reg: store.getPage(2 * origin.pageNum as PageNum),
            del: store.getPage(2 * origin.pageNum + 1 as PageNum),
        };
    }
}

/**
 * A collection that lets you create a copy-on-write writable shapshot of itself.
 *
 * We use deletion markers on the snapshot stores, so new pages on the origin take 2x
 * their normal usage, whereas new pages on the snapshot only take 1x. These markers are
 * removed if the origin page is also deleted to keep WAL rotation from exploding disk
 * usage.
 */
export class CowCollection implements IStoreCollection<CowPage, CowStore> {
    public readonly pageSize: PageSize;

    private inner: IStoreCollection<IPage, IPageStore<IPage>>;
    private state: CowState;
    private map = new ShMap<CowPage, CowStore>();

    public constructor(inner: IStoreCollection<IPage, IPageStore<IPage>>) {
        this.state = new CowState();
        this.inner = inner;
        this.pageSize = inner.pageSize;
    }

    /**
     * Creates a snapshot of the store, keeping writes on a new collection.
     *
     * This method returns a new collection that contains the contents of the original
     * at the moment in time it was created.
     *
     * Open handles are not copied over to the snapshot. It behaves as if all handles
     * on the original were closed before the snapshot was taken.
     */
    public snapshot(on: IStoreCollection<IPage, IPageStore<IPage>>): RefCowCollection {
        this.state.ref = on;
        return new RefCowCollection(this.inner, on);
    }

    /**
     * Detaches the current snapshot collection.
     *
     * The associated RefCowCollection's contents become arbitrary after this operation
     * and shouldn't be relied upon.
     */
    public detach(): void {
        this.state.ref = undefined;
    }

    public getStore(namespace: Namespace): CowStore {
        return this.map.getStore(namespace, () => new CowStore(
            this.state,
            namespace,
            this.inner.getStore(namespace),
            this.map,
        ));
    }

    public listStores(): LuaSet<Namespace> {
        return this.inner.listStores();
    }
}

export class CowStore implements IPageStore<CowPage> {
    public readonly pageSize: PageSize;
    public readonly namespace: Namespace;

    private inner: IPageStore<IPage>;
    private state: CowState;
    private map: ShMap<CowPage, CowStore>;

    public constructor(
        state: CowState,
        namespace: Namespace,
        inner: IPageStore<IPage>,
        map: ShMap<CowPage, CowStore>,
    ) {
        this.state = state;
        this.inner = inner;
        this.namespace = namespace;
        this.pageSize = inner.pageSize;
        this.map = map;
    }

    public getPage(pageNum: PageNum): CowPage {
        return this.map.getPage(this.namespace, pageNum, () => new CowPage(
            this.state,
            this.namespace,
            this.inner.getPage(pageNum),
        ));
    }

    public listPages(): LuaSet<PageNum> {
        return this.inner.listPages();
    }
}

export class CowPage implements IPage {
    public readonly pageSize: PageSize;
    public readonly namespace: Namespace;
    public readonly pageNum: PageNum;

    private inner: IPage;
    private state: CowState;

    public constructor(state: CowState, namespace: Namespace, inner: IPage) {
        this.state = state;
        this.namespace = namespace;
        this.inner = inner;
        this.pageSize = inner.pageSize;
        this.pageNum = inner.pageNum;
    }

    /** Copies a shared page over to the ref store, in preparation for a write. */
    private clone(): void {
        const ref = this.state.getRef(this);
        if (!ref) { return; }
        if (ref.reg.exists() || ref.del.exists()) { return; }
        if (this.exists()) {
            ref.reg.create(this.read());
        } else {
            ref.del.create();
        }
    }

    public exists(): boolean {
        return this.inner.exists();
    }

    public create(initialData?: string): void {
        this.clone();
        return this.inner.create(initialData);
    }

    public createOpen(): void {
        this.clone();
        return this.inner.createOpen();
    }

    public delete(): void {
        this.clone();
        const ref = this.state.getRef(this);
        // We don't need to keep a del ref page if the origin page doesn't exist either.
        if (ref && ref.del.exists()) { ref.del.delete(); }
        return this.inner.delete();
    }

    public read(): string | undefined {
        return this.inner.read();
    }

    public write(data: string): void {
        this.clone();
        return this.inner.write(data);
    }

    public append(extra: string): void {
        this.clone();
        return this.inner.append(extra);
    }

    public canAppend(): boolean {
        return this.inner.canAppend();
    }

    public openAppend(): void {
        this.clone();
        return this.inner.openAppend();
    }

    public closeAppend(): void {
        this.clone();
        return this.inner.closeAppend();
    }
}

/** A writable snapshot of an existing CowCollection. */
export class RefCowCollection implements IStoreCollection<RefCowPage, RefCowStore> {
    public readonly pageSize: PageSize;

    /** The collection containing the original pages. */
    private origin: IStoreCollection<IPage, IPageStore<IPage>>;

    /** The collection containing the CoW write pages. */
    private ref: IStoreCollection<IPage, IPageStore<IPage>>;

    private map = new ShMap<RefCowPage, RefCowStore>();

    public constructor(
        origin: IStoreCollection<IPage, IPageStore<IPage>>,
        ref: IStoreCollection<IPage, IPageStore<IPage>>,
    ) {
        this.origin = origin;
        this.ref = ref;
        this.pageSize = ref.pageSize;
    }

    public getStore(namespace: Namespace): RefCowStore {
        return this.map.getStore(namespace, () => {
            const origin = this.origin.getStore(namespace);
            const ref = this.ref.getStore(namespace);
            return new RefCowStore(namespace, origin, ref, this.map);
        });
    }

    public listStores(): LuaSet<Namespace> {
        // This is ugly but it's the best we have.
        const stores = this.origin.listStores();
        for (const store of this.ref.listStores()) { stores.add(store); }
        for (const store of stores) {
            const pages = this.getStore(store).listPages();
            if (pages.isEmpty()) { stores.delete(store); }
        }
        return stores;
    }
}

export class RefCowStore implements IPageStore<RefCowPage> {
    public readonly pageSize: PageSize;

    private namespace: Namespace;
    private origin: IPageStore<IPage>;
    private ref: IPageStore<IPage>;
    private map: ShMap<RefCowPage, RefCowStore>;

    public constructor(
        namespace: Namespace,
        origin: IPageStore<IPage>,
        ref: IPageStore<IPage>,
        map: ShMap<RefCowPage, RefCowStore>,
    ) {
        this.namespace = namespace;
        this.origin = origin;
        this.ref = ref;
        this.map = map;
        this.pageSize = ref.pageSize;
    }

    public getPage(pageNum: PageNum): RefCowPage {
        return this.map.getPage(this.namespace, pageNum, () => {
            const origin = this.origin.getPage(pageNum);
            const refReg = this.ref.getPage(2 * pageNum as PageNum);
            const refDel = this.ref.getPage(2 * pageNum + 1 as PageNum);
            return new RefCowPage(origin, refReg, refDel);
        });
    }

    public listPages(): LuaSet<PageNum> {
        const out = this.origin.listPages();
        for (const refPage of this.ref.listPages()) {
            if (refPage % 2 == 0) {
                out.add(refPage / 2 as PageNum);
            } else {
                out.delete((refPage - 1) / 2 as PageNum);
            }
        }
        return out;
    }
}

export class RefCowPage implements IPage {
    public readonly pageSize: PageSize;
    public readonly pageNum: PageNum;

    private origin: IPage;
    private refReg: IPage;
    private refDel: IPage;

    public constructor(origin: IPage, refReg: IPage, refDel: IPage) {
        this.origin = origin;
        this.refReg = refReg;
        this.refDel = refDel;
        this.pageSize = refReg.pageSize;
        this.pageNum = refReg.pageNum / 2 as PageNum;
    }

    /** Copies a shared page over from the origin store, in preparation for a write. */
    private clone(): void {
        if (this.refReg.exists() || this.refDel.exists()) { return; }
        if (this.origin.exists()) {
            this.refReg.create(this.origin.read());
        } else {
            this.refDel.create();
        }
    }

    public exists(): boolean {
        if (this.refDel.exists()) { return false; }
        if (this.refReg.exists()) { return true; }
        return this.origin.exists();
    }

    public create(initialData?: string | undefined): void {
        this.clone();
        this.refDel.delete();
        this.refReg.create(initialData);
    }

    public createOpen(): void {
        this.clone();
        this.refDel.delete();
        this.refReg.createOpen();
    }

    public delete(): void {
        this.clone();
        this.refReg.delete();
        this.refDel.create();
    }

    public read(): string | undefined {
        if (this.refDel.exists()) {
            return;
        } else if (this.refReg.exists()) {
            return this.refReg.read();
        } else {
            return this.origin.read();
        }
    }

    public write(data: string): void {
        this.clone();
        this.refReg.write(data);
    }

    public append(extra: string): void {
        this.clone();
        this.refReg.append(extra);
    }

    public canAppend(): boolean {
        return this.refReg.canAppend();
    }

    public openAppend(): void {
        this.clone();
        return this.refReg.openAppend();
    }

    public closeAppend(): void {
        this.clone();
        return this.refReg.closeAppend();
    }
}
