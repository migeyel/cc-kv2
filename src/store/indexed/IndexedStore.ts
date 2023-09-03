import { ObjCache } from "../../ObjCache";
import { Deque, DequeNode } from "../../Deque";
import { IPage, IPageStore, IStoreCollection } from "../IPageStore";
import { IndexCollection, IndexPage } from "./Index";
import { IndexMetaPage, ProcedureType, SubMeta, Uuid } from "./Meta";

/** A store collection that multiplexes store collections through an index. */
export class IndexedCollection implements IStoreCollection<
    IndexedPage,
    IndexedStore
> {
    public readonly pageSize: number;

    private state: IndexState;

    // We need to share stores because they need to share pages.
    private stores: ObjCache<string, IndexedStore>;

    public constructor(
        pageSize: number,
        cacheSize: number,
        storedMetaPage: IPage,
        indexStore: IStoreCollection<IPage, IPageStore<IPage>>,
        subStores: LuaMap<Uuid, IStoreCollection<IPage, IPageStore<IPage>>>,
    ) {
        const getter = (namespace: string) => {
            return new IndexedStore(
                cacheSize,
                this.state,
                this.pageSize,
                namespace,
            );
        };

        this.stores = new ObjCache(cacheSize, getter);
        this.pageSize = pageSize;
        this.state = new IndexState(
            cacheSize,
            storedMetaPage,
            indexStore,
            subStores,
        );

        this.recoverPartialPage();
        this.recoverMovePage();
    }

    private recoverPartialPage() {
        const state = this.state;
        const proc = state.storedMeta.proc;
        if (!proc || proc.ty != ProcedureType.PARTIAL_PAGE) { return; }

        // Check if it is in the index.
        const indexStore = state.indexCollection.getIndexStore(proc.namespace);
        const indexPage = indexStore.getPageIndexPage(proc.pageNum);
        const num = indexPage.getPageSubNum(proc.pageNum);
        if (!num) {
            // Because of procedure ordering, if the page isn't in the index it
            // also isn't in the store, so we're done.
            state.storedMeta.proc = undefined;
            state.storedMeta.commit();
            return;
        }

        // Check if it's in the store.
        const uuid = assert(state.subStorePerNum.get(num));
        const sub = assert(state.subStores.get(uuid));
        const page = sub.getStore(proc.namespace).getPage(proc.pageNum);
        if (page.exists()) {
            // It's in both places so we just have to increment the usage.
            state.incrStoreUsed(uuid);
            state.storedMeta.proc = undefined;
            state.storedMeta.commit();
            return;
        }

        // It's in the index but not the directory, so we remove from the index.
        indexPage.delPageSubNum(proc.pageNum);
        indexPage.save();
        state.storedMeta.proc = undefined;
        state.storedMeta.commit();
    }

    private recoverMovePage() {
        const state = this.state;
        const proc = state.storedMeta.proc;
        if (!proc || proc.ty != ProcedureType.MOVE_PAGE) { return; }

        // Check what the index says.
        const indexStore = state.indexCollection.getIndexStore(proc.namespace);
        const indexPage = indexStore.getPageIndexPage(proc.pageNum);
        const num = assert(indexPage.getPageSubNum(proc.pageNum));
        const uuid = assert(state.subStorePerNum.get(num));
        if (uuid == proc.sourceUuid) {
            // If it's still in the source we're done.
            state.storedMeta.proc = undefined;
            state.storedMeta.commit();
            return;
        }

        // Check if there's a copy in the source dir.
        const srcSub = assert(state.subStores.get(proc.sourceUuid));
        const srcPage = srcSub.getStore(proc.namespace).getPage(proc.pageNum);
        if (!srcPage.exists()) {
            // It's not there so the copy was a success. We just need to update
            // the metadata.
            state.decrStoreUsed(proc.sourceUuid);
            state.incrStoreUsed(proc.targetUuid);
            state.storedMeta.proc = undefined;
            state.storedMeta.commit();
            return;
        }

        // Check if there's a copy in the target dir.
        const tgtSub = assert(state.subStores.get(proc.targetUuid));
        const tgtPage = tgtSub.getStore(proc.namespace).getPage(proc.pageNum);
        if (!tgtPage.exists()) {
            // Copy the page over.
            tgtPage.create(srcPage.read());
        }

        // Delete the source page and update the metadata.
        srcPage.delete();
        state.decrStoreUsed(proc.sourceUuid);
        state.incrStoreUsed(proc.targetUuid);
        state.storedMeta.proc = undefined;
        state.storedMeta.commit();
    }

    public getStore(namespace: string): IndexedStore {
        return this.stores.get(namespace);
    }

    public listStores(): LuaSet<string> {
        const out = new LuaSet<string>();
        for (const [_, sub] of this.state.subStores) {
            for (const page of sub.listStores()) {
                out.add(page);
            }
        }
        return out;
    }

    public addSubStore(
        uuid: Uuid,
        store: IStoreCollection<IPage, IPageStore<IPage>>,
        quota: number,
    ) {
        const state = this.state;
        assert(store.pageSize <= this.pageSize, "invalid sub-store page size");
        assert(!state.subStores.has(uuid), "can't add sub-store twice");
        let indexNumber = 1;
        while (state.subStorePerNum.has(indexNumber)) { indexNumber++; }
        const meta = { indexNumber, allocatedQuota: quota, numAllocated: 0 };
        state.subStorePerNum.set(indexNumber, uuid);
        state.subStores.set(uuid, store);
        state.storedMeta.subs.set(uuid, meta);
        state.addToQueue(uuid, meta);
        state.storedMeta.commit();
    }

    public removeSubStore(uuid: Uuid) {
        const state = this.state;
        const sub = state.subStores.get(uuid);
        if (!sub) { return; }
        const meta = assert(state.storedMeta.subs.get(uuid));
        assert(meta.numAllocated == 0, "can't delete a nonempty store");
        state.delFromQueue(uuid);
        state.storedMeta.subs.delete(uuid);
        state.subStores.delete(uuid);
        state.subStorePerNum.delete(meta.indexNumber);
        state.storedMeta.commit();
    }
}

class IndexedStore implements IPageStore<IndexedPage> {
    public readonly pageSize: number;

    private namespace: string;

    private state: IndexState;

    // We need to share pages because they reflect global disk state (location
    // of the page in a store, and whether the page can be appended).
    private pages: ObjCache<number, IndexedPage>;

    public constructor(
        cacheSize: number,
        state: IndexState,
        pageSize: number,
        namespace: string,
    ) {
        const getter = (pageNum: number) => {
            return new IndexedPage(
                this.state,
                this.pageSize,
                this.namespace,
                pageNum,
            );
        };

        this.pages = new ObjCache(cacheSize, getter);
        this.state = state;
        this.pageSize = pageSize;
        this.namespace = namespace;
    }

    public getPage(pageNum: number): IndexedPage {
        return this.pages.get(pageNum);
    }

    public listPages(): LuaSet<number> {
        const out = new LuaSet<number>();
        for (const [_, sub] of this.state.subStores) {
            for (const page of sub.getStore(this.namespace).listPages()) {
                out.add(page);
            }
        }
        return out;
    }
}

class IndexedPage implements IPage {
    public readonly pageSize: number;

    public readonly pageNum: number;

    private namespace: string;

    /** The index page that resolves this page's number. */
    private indexPage: IndexPage;

    /** The underlying page, iff it is allocated. */
    private page?: IPage;

    /** Refcount for the append handle. We need it since pages are shared. */
    private appendRefCount = 0;

    private state: IndexState;

    public constructor(
        state: IndexState,
        pageSize: number,
        namespace: string,
        pageNum: number,
    ) {
        this.state = state;
        this.pageSize = pageSize;
        this.namespace = namespace;
        this.pageNum = pageNum;
        const indexStore = state.indexCollection.getIndexStore(namespace);
        this.indexPage = indexStore.getPageIndexPage(pageNum);
        const num = this.indexPage.getPageSubNum(this.pageNum);
        if (!num) { return; }
        const uuid = assert(state.subStorePerNum.get(num));
        const sub = assert(state.subStores.get(uuid));
        this.page = sub.getStore(namespace).getPage(pageNum);
    }

    public exists(): boolean {
        return this.page != undefined;
    }

    public create(initialData?: string | undefined): void {
        assert(!this.exists(), "page exists");
        const state = this.state;
        const node = state.freeQueue.first();
        if (!node) { throw new Error("out of space"); }
        const uuid = node.val;
        const meta = assert(state.storedMeta.subs.get(uuid));
        if (meta.numAllocated >= meta.allocatedQuota) {
            throw new Error("out of space");
        }

        const sub = assert(state.subStores.get(uuid)).getStore(this.namespace);

        // Register the partial page procedure.
        state.storedMeta.proc = {
            ty: ProcedureType.PARTIAL_PAGE,
            namespace: this.namespace,
            pageNum: this.pageNum,
        };
        state.storedMeta.commit();

        // Write (index, then directory).
        this.indexPage.setPageSubNum(this.pageNum, meta.indexNumber);
        this.indexPage.save();
        this.page = sub.getPage(this.pageNum);
        this.page.create(initialData);

        // Finish the procedure in memory.
        state.incrStoreUsed(uuid);
        state.storedMeta.proc = undefined;
    }

    public createOpen(): void {
        assert(!this.exists(), "page exists");
        const state = this.state;
        const node = state.freeQueue.first();
        if (!node) { throw new Error("out of space"); }
        const uuid = node.val;
        const meta = assert(state.storedMeta.subs.get(uuid));
        if (meta.numAllocated >= meta.allocatedQuota) {
            throw new Error("out of space");
        }

        const sub = assert(state.subStores.get(uuid)).getStore(this.namespace);

        // Register the partial page procedure.
        state.storedMeta.proc = {
            ty: ProcedureType.PARTIAL_PAGE,
            namespace: this.namespace,
            pageNum: this.pageNum,
        };
        state.storedMeta.commit();

        // Write (index, then directory).
        this.indexPage.setPageSubNum(this.pageNum, meta.indexNumber);
        this.indexPage.save();
        this.page = sub.getPage(this.pageNum);
        this.page.createOpen();
        this.appendRefCount = 1;

        // Finish the procedure in memory.
        state.incrStoreUsed(uuid);
        state.storedMeta.proc = undefined;
    }

    public delete(): void {
        const [page] = assert(this.page, "page doesn't exist");
        const state = this.state;
        const num = assert(this.indexPage.getPageSubNum(this.pageNum));
        const uuid = assert(state.subStorePerNum.get(num));

        // Register the partial page procedure.
        state.decrStoreUsed(uuid);
        state.storedMeta.proc = {
            ty: ProcedureType.PARTIAL_PAGE,
            namespace: this.namespace,
            pageNum: this.pageNum,
        };

        // Delete (directory, then index).
        if (this.appendRefCount > 0) { page.closeAppend(); }
        page.delete();
        this.indexPage.delPageSubNum(this.pageNum);
        this.indexPage.save();

        // Finish the procedure in memory.
        state.storedMeta.proc = undefined;
        this.page = undefined;
        this.appendRefCount = 0;
    }

    public move(tgtUuid: Uuid) {
        const [srcPage] = assert(this.page, "page doesn't exist");
        const state = this.state;
        const tgtMeta = assert(state.storedMeta.subs.get(tgtUuid));
        if (tgtMeta.numAllocated >= tgtMeta.allocatedQuota) {
            throw new Error("out of space");
        }
        const tgtSub = assert(state.subStores.get(tgtUuid));
        const srcNum = assert(this.indexPage.getPageSubNum(this.pageNum));
        const srcUuid = assert(state.subStorePerNum.get(srcNum));
        const tgtPage = tgtSub.getStore(this.namespace).getPage(this.pageNum);

        // Register the move page procedure.
        state.storedMeta.proc = {
            ty: ProcedureType.MOVE_PAGE,
            sourceUuid: srcUuid,
            targetUuid: tgtUuid,
            namespace: this.namespace,
            pageNum: this.pageNum,
        };
        state.storedMeta.commit();

        // Update the index.
        this.indexPage.setPageSubNum(this.pageNum, tgtMeta.indexNumber);
        this.indexPage.save();

        // Copy the page over.
        if (this.appendRefCount > 0) { srcPage.closeAppend(); }
        tgtPage.create(srcPage.read());
        srcPage.delete();

        // Finish the procedure in memory.
        state.decrStoreUsed(srcUuid);
        state.incrStoreUsed(tgtUuid);
        state.storedMeta.proc = undefined;
        this.page = tgtPage;
        if (this.appendRefCount > 0) { tgtPage.openAppend(); }
    }

    public read(): string | undefined {
        return this.page?.read();
    }

    public write(data: string): void {
        return this.page?.write(data);
    }

    public append(extra: string): void {
        assert(this.appendRefCount > 0);
        assert(this.page).append(extra);
    }

    public canAppend(): boolean {
        return this.appendRefCount > 0;
    }

    public openAppend(): void {
        const page = assert(this.page);
        if (this.appendRefCount++ == 1) {
            page.openAppend();
        }
    }

    public closeAppend(): void {
        const page = assert(this.page);
        assert(this.appendRefCount > 0);
        if (this.appendRefCount-- == 0) {
            page.closeAppend();
        }
    }

    public flush(): void {
        assert(this.page).flush();
    }
}

class IndexState {
    public storedMeta: IndexMetaPage;
    public indexCollection: IndexCollection;
    public subStores: LuaMap<Uuid, IStoreCollection<IPage, IPageStore<IPage>>>;
    public subStorePerNum: LuaMap<number, Uuid>;
    public freeQueue: Deque<Uuid>;
    public freeQueueMap: LuaMap<Uuid, DequeNode<Uuid>>;

    public constructor(
        cacheSize: number,
        storedMetaPage: IPage,
        indexCollection: IStoreCollection<IPage, IPageStore<IPage>>,
        subStores: LuaMap<Uuid, IStoreCollection<IPage, IPageStore<IPage>>>,
    ) {
        this.storedMeta = new IndexMetaPage(storedMetaPage);
        this.indexCollection = new IndexCollection(cacheSize, indexCollection);
        this.subStores = new LuaMap();
        this.subStorePerNum = new LuaMap();
        this.freeQueue = new Deque();
        this.freeQueueMap = new LuaMap();
        for (const [uuid, meta] of this.storedMeta.subs) {
            const [sub] = assert(subStores.get(uuid), "missing store " + uuid);
            this.subStores.set(uuid, sub);
            this.subStorePerNum.set(meta.indexNumber, uuid);
            this.addToQueue(uuid, meta);
        }
    }

    public addToQueue(uuid: Uuid, meta: SubMeta) {
        const free = meta.allocatedQuota - meta.numAllocated;
        let node = this.freeQueue.first();
        while (node) {
            const nodeMeta = assert(this.storedMeta.subs.get(node.val));
            const nodeFree = nodeMeta.allocatedQuota - nodeMeta.numAllocated;
            if (nodeFree <= free) {
                this.freeQueueMap.set(uuid, node.pushBefore(uuid));
                return;
            }
            node = node.getNext();
        }
        this.freeQueueMap.set(uuid, this.freeQueue.pushBack(uuid));
    }

    public delFromQueue(uuid: Uuid) {
        this.freeQueueMap.delete(assert(this.freeQueueMap.get(uuid)).pop());
    }

    public incrStoreUsed(uuid: Uuid) {
        const meta = assert(this.storedMeta.subs.get(uuid));
        const free = meta.allocatedQuota - meta.numAllocated++;
        const node = assert(this.freeQueueMap.get(uuid));
        let next = node.getNext();
        node.pop();
        while (next) {
            const nextMeta = assert(this.storedMeta.subs.get(node.val));
            const nextFree = nextMeta.allocatedQuota - nextMeta.numAllocated;
            if (nextFree <= free) {
                this.freeQueueMap.set(uuid, next.pushBefore(uuid));
                return;
            }
            next = next.getNext();
        }
        this.freeQueueMap.set(uuid, this.freeQueue.pushBack(uuid));
    }

    public decrStoreUsed(uuid: Uuid) {
        const meta = assert(this.storedMeta.subs.get(uuid));
        const free = meta.allocatedQuota - meta.numAllocated--;
        const node = assert(this.freeQueueMap.get(uuid));
        let prev = node.getPrev();
        node.pop();
        while (prev) {
            const prevMeta = assert(this.storedMeta.subs.get(node.val));
            const prevFree = prevMeta.allocatedQuota - prevMeta.numAllocated;
            if (prevFree >= free) {
                this.freeQueueMap.set(uuid, prev.pushAfter(uuid));
                return;
            }
            prev = prev.getPrev();
        }
        this.freeQueueMap.set(uuid, this.freeQueue.pushBack(uuid));
    }
}
