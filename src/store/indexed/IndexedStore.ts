import { Deque, DequeNode } from "../../Deque";
import {
    IPage,
    IPageStore,
    IStoreCollection,
    MAX_NAMESPACE_LEN,
    Namespace,
    PageNum,
    PageSize,
} from "../IPageStore";
import { IndexCollection, IndexPage, MAX_INDEXED_SUBSTORES } from "./Index";
import { RecordLog } from "../../RecordLog";
import { IndexLog, RecordType, SubStoreNum } from "./IndexLog";
import { ShMap } from "../../ShMap";

export type SubStore = {
    collection: IStoreCollection<IPage, IPageStore<IPage>>,
    quota: number,
};

/** A store collection that multiplexes store collections through an index. */
export class IndexedCollection implements IStoreCollection<
    IndexedPage,
    IndexedStore
> {
    public readonly pageSize: PageSize;

    private state: IndexState;

    private stores = new ShMap<Namespace, IndexedStore>();

    public constructor(
        pageSize: PageSize,
        cacheSize: number,
        indexLog: RecordLog,
        indexCollection: IStoreCollection<IPage, IPageStore<IPage>>,
        subStores: LuaMap<SubStoreNum, SubStore>,
    ) {
        this.pageSize = pageSize;
        this.state = new IndexState(
            indexLog,
            indexCollection,
            subStores,
        );

        this.state.recoverLastProcedure();
    }

    public static repopulateIndex(
        indexLog: RecordLog,
        indexCollection: IStoreCollection<IPage, IPageStore<IPage>>,
        subStores: LuaMap<SubStoreNum, SubStore>,
    ) {
        // Wrap the collection as some index state for encoding.
        const state = new IndexState(
            indexLog,
            indexCollection,
            subStores,
        );

        // Repopulate the index from the store listing. Recovery *should* take
        // care of everything else.
        for (const [num, sub] of subStores) {
            const coll = sub.collection;
            for (const namespace of coll.listStores()) {
                const indexStore =
                    state.indexCollection.getIndexStore(namespace);
                const store = coll.getStore(namespace);
                for (const pageNum of store.listPages()) {
                    const page = indexStore.getPageIndexPage(pageNum);
                    page.setPageSubNum(pageNum, num);
                    page.save();
                }
            }
        }
    }

    public getStore(namespace: Namespace): IndexedStore {
        assert(namespace.length <= MAX_NAMESPACE_LEN);
        return this.stores.getOr(namespace, () => new IndexedStore(
            this.state,
            this.pageSize,
            namespace,
        ));
    }

    public listStores(): LuaSet<Namespace> {
        const out = new LuaSet<Namespace>();
        for (const [_, sub] of this.state.subStores) {
            for (const page of sub.listStores()) {
                out.add(page);
            }
        }
        return out;
    }

    public addSubStore(
        num: SubStoreNum,
        store: IStoreCollection<IPage, IPageStore<IPage>>,
        quota: number,
    ) {
        assert(num > 0, "invalid sub-store number");
        assert(num <= MAX_INDEXED_SUBSTORES, "invalid sub-store number");
        const state = this.state;
        assert(store.pageSize >= this.pageSize, "invalid sub-store page size");
        assert(!state.subStores.has(num), "can't add sub-store twice");
        state.subStores.set(num, store);
        state.quotas.set(num, quota);
        state.log.registerProcedure({
            ty: RecordType.CREATE_SUB_STORE,
            where: num,
        });
        state.updateQueue(num);
        state.log.writeCheckpointIfFull();
    }

    public removeSubStore(num: SubStoreNum) {
        const state = this.state;
        const usage = state.log.usages.get(num);
        if (!usage) { return; }
        assert(usage == 0, "can't delete a nonempty store");
        state.subStores.delete(num);
        state.quotas.delete(num);
        state.log.registerProcedure({
            ty: RecordType.DELETE_SUB_STORE,
            where: num,
        });
        state.updateQueue(num);
        state.log.writeCheckpointIfFull();
    }
}

class IndexedStore implements IPageStore<IndexedPage> {
    public readonly pageSize: PageSize;

    private namespace: Namespace;

    private state: IndexState;

    private pages = new ShMap<PageNum, IndexedPage>();

    public constructor(
        state: IndexState,
        pageSize: PageSize,
        namespace: Namespace,
    ) {
        this.state = state;
        this.pageSize = pageSize;
        this.namespace = namespace;
    }

    public getPage(pageNum: PageNum): IndexedPage {
        return this.pages.getOr(pageNum, () => new IndexedPage(
            this.state,
            this.pageSize,
            this.namespace,
            pageNum,
        ));
    }

    public listPages(): LuaSet<PageNum> {
        const out = new LuaSet<PageNum>();
        for (const [_, sub] of this.state.subStores) {
            for (const page of sub.getStore(this.namespace).listPages()) {
                out.add(page);
            }
        }
        return out;
    }
}

class IndexedPage implements IPage {
    public readonly pageSize: PageSize;

    public readonly pageNum: PageNum;

    private namespace: Namespace;

    /** The index page that resolves this page's number. */
    private indexPage: IndexPage;

    /** The underlying page, iff it is allocated. */
    private page?: IPage;

    private state: IndexState;

    public constructor(
        state: IndexState,
        pageSize: PageSize,
        namespace: Namespace,
        pageNum: PageNum,
    ) {
        this.state = state;
        this.pageSize = pageSize;
        this.namespace = namespace;
        this.pageNum = pageNum;
        const indexStore = state.indexCollection.getIndexStore(namespace);
        this.indexPage = indexStore.getPageIndexPage(pageNum);
        const num = this.indexPage.getPageSubNum(this.pageNum);
        if (!num) { return; }
        const sub = assert(state.subStores.get(num));
        this.page = sub.getStore(namespace).getPage(pageNum);
    }

    public exists(): boolean {
        return this.page != undefined;
    }

    public create(initialData?: string | undefined): void {
        assert(!this.exists(), "page exists");
        const state = this.state;
        const [node] = assert(state.freeQueue.first(), "out of space");
        const where = node.val;
        const allocated = assert(this.state.log.usages.get(where));
        const quota = assert(this.state.quotas.get(where));
        assert(allocated < quota, "out of space");
        const sub = assert(state.subStores.get(where));
        const store = sub.getStore(this.namespace);
        const page = store.getPage(this.pageNum);

        // Register the partial page procedure.
        state.log.registerProcedure({
            ty: RecordType.CREATE_PAGE,
            namespace: this.namespace,
            pageNum: this.pageNum,
            where,
        });
        state.updateQueue(where);

        // Write (index, then directory).
        this.indexPage.setPageSubNum(this.pageNum, where);
        this.indexPage.save();
        this.page = page;
        this.page.create(initialData);

        // Finish the procedure in memory.
        state.log.writeCheckpointIfFull();
    }

    public createOpen(): void {
        assert(!this.exists(), "page exists");
        const state = this.state;
        const [node] = assert(state.freeQueue.first(), "out of space");
        const where = node.val;
        const allocated = assert(this.state.log.usages.get(where));
        const quota = assert(this.state.quotas.get(where));
        assert(allocated < quota, "out of space");
        const sub = assert(state.subStores.get(where));
        const store = sub.getStore(this.namespace);
        const page = store.getPage(this.pageNum);

        // Register the partial page procedure.
        state.log.registerProcedure({
            ty: RecordType.CREATE_PAGE,
            namespace: this.namespace,
            pageNum: this.pageNum,
            where,
        });
        state.updateQueue(where);

        // Write (index, then directory).
        this.indexPage.setPageSubNum(this.pageNum, where);
        this.indexPage.save();
        this.page = page;
        this.page.createOpen();

        // Finish the procedure in memory.
        state.log.writeCheckpointIfFull();
    }

    public delete(): void {
        const [page] = assert(this.page, "page doesn't exist");
        assert(!page.canAppend(), "page is open for appending");
        const state = this.state;
        const where = assert(this.indexPage.getPageSubNum(this.pageNum));

        // Register the partial page procedure.
        state.log.registerProcedure({
            ty: RecordType.DELETE_PAGE,
            namespace: this.namespace,
            pageNum: this.pageNum,
            where,
        });
        state.updateQueue(where);

        // Delete (directory, then index).
        page.delete();
        this.indexPage.delPageSubNum(this.pageNum);
        this.indexPage.save();

        // Finish the procedure in memory.
        this.page = undefined;
        state.log.writeCheckpointIfFull();
    }

    /** Moves a page to another sub-store. */
    public move(target: SubStoreNum) {
        const [srcPage] = assert(this.page, "page doesn't exist");
        const state = this.state;
        const usage = assert(state.log.usages.get(target));
        const quota = assert(state.quotas.get(target));
        assert(usage < quota, "out of space");
        const tgtSub = assert(state.subStores.get(target));
        const tgtStore = tgtSub.getStore(this.namespace);
        const tgtPage = tgtStore.getPage(this.pageNum);
        const source = assert(this.indexPage.getPageSubNum(this.pageNum));

        // Register the move page procedure.
        state.log.registerProcedure({
            ty: RecordType.MOVE_PAGE,
            from: source,
            to: target,
            namespace: this.namespace,
            pageNum: this.pageNum,
        });
        state.updateQueue(source, target);

        // Update the index.
        this.indexPage.setPageSubNum(this.pageNum, target);
        this.indexPage.save();

        // Copy the page over.
        const srcCanAppend = srcPage.canAppend();
        if (srcCanAppend) { srcPage.closeAppend(); }
        tgtPage.create(srcPage.read());
        srcPage.delete();

        // Finish the procedure in memory.
        this.page = tgtPage;
        if (srcCanAppend) { tgtPage.openAppend(); }
        state.log.writeCheckpointIfFull();
    }

    public read(): string | undefined {
        return this.page?.read();
    }

    public write(data: string): void {
        const [page] = assert(this.page, "page doesn't exist");
        assert(!page.canAppend(), "page is open for appending");
        return page?.write(data);
    }

    public append(extra: string): void {
        const [page] = assert(this.page, "page doesn't exist");
        assert(page.canAppend(), "page isn't open for appending");
        page.append(extra);
    }

    public canAppend(): boolean {
        const page = this.page;
        if (!page) { return false; }
        return page.canAppend();
    }

    public openAppend(): void {
        const [page] = assert(this.page, "page doesn't exist");
        assert(!page.canAppend(), "page is open for appending");
        page.openAppend();
    }

    public closeAppend(): void {
        const [page] = assert(this.page, "page doesn't exist");
        assert(page.canAppend(), "page isn't open for appending");
        page.openAppend();
    }

    public flush(): void {
        assert(this.page).flush();
    }
}

class IndexState {
    public log: IndexLog;
    public indexCollection: IndexCollection;
    public freeQueue: Deque<SubStoreNum>;
    public freeQueueMap: LuaMap<SubStoreNum, DequeNode<SubStoreNum>>;
    public quotas: LuaMap<SubStoreNum, number>;

    public subStores: LuaMap<
        SubStoreNum,
        IStoreCollection<IPage, IPageStore<IPage>>
    >;

    public constructor(
        indexLog: RecordLog,
        indexCollection: IStoreCollection<IPage, IPageStore<IPage>>,
        subStores: LuaMap<SubStoreNum, SubStore>,
    ) {
        this.log = new IndexLog(indexLog);
        this.indexCollection = new IndexCollection(indexCollection);
        this.subStores = new LuaMap();
        this.quotas = new LuaMap();
        this.freeQueue = new Deque();
        this.freeQueueMap = new LuaMap();
        for (const [num] of this.log.usages) {
            const [sub] = assert(subStores.get(num), "missing store " + num);
            this.subStores.set(num, sub.collection);
            this.quotas.set(num, sub.quota);
            this.updateQueue(num);
        }
    }

    /** Updates the free node queue to reflect the log usage. */
    public updateQueue(...nums: SubStoreNum[]) {
        for (const num of nums) {
            if (this.freeQueueMap.has(num)) {
                assert(this.freeQueueMap.get(num)).pop();
                this.freeQueueMap.delete(num);
            }
        }

        for (const num of nums) {
            const quota = assert(this.quotas.get(num));
            const usage = assert(this.log.usages.get(num));
            const free = quota - usage;
            if (usage) {
                let node = this.freeQueue.first();
                while (node) {
                    const nodeQuota = assert(this.quotas.get(node.val));
                    const nodeUsage = assert(this.log.usages.get(node.val));
                    const nodeFree = nodeQuota - nodeUsage;
                    if (nodeFree <= free) {
                        this.freeQueueMap.set(num, node.pushBefore(num));
                        return;
                    }
                    node = node.getNext();
                }
                this.freeQueueMap.set(num, this.freeQueue.pushBack(num));
            }
        }
    }

    public recoverLastProcedure() {
        const proc = this.log.lastProcedure;
        if (!proc) { return; }
        if (proc.ty == RecordType.CREATE_PAGE) {
            // Commits are signalled by the target existing. Rollback otherwise.
            const coll = assert(this.subStores.get(proc.where));
            const store = coll.getStore(proc.namespace);
            const page = store.getPage(proc.pageNum);
            if (!page.exists) {
                // No file, append a delete procedure and recover again.
                this.log.registerProcedure({
                    ty: RecordType.DELETE_PAGE,
                    namespace: proc.namespace,
                    pageNum: proc.pageNum,
                    where: proc.where,
                });
                this.updateQueue(proc.where);
                this.recoverLastProcedure();
            }
        } else if (proc.ty == RecordType.DELETE_PAGE) {
            // Always commit.
            const coll = assert(this.subStores.get(proc.where));
            const store = coll.getStore(proc.namespace);
            const page = store.getPage(proc.pageNum);
            if (page.exists()) { page.delete(); }
            const indexPage = this.indexCollection
                .getIndexStore(proc.namespace)
                .getPageIndexPage(proc.pageNum);
            indexPage.delPageSubNum(proc.pageNum);
            indexPage.save();
        } else if (proc.ty == RecordType.MOVE_PAGE) {
            // Commits are signalled by the target existing. Redo otherwise.
            const srcColl = assert(this.subStores.get(proc.from));
            const srcStore = srcColl.getStore(proc.namespace);
            const srcPage = srcStore.getPage(proc.pageNum);
            const tgtColl = assert(this.subStores.get(proc.to));
            const tgtStore = tgtColl.getStore(proc.namespace);
            const tgtPage = tgtStore.getPage(proc.pageNum);
            if (!tgtPage.exists()) {
                // No target file, set the index then copy it over.
                const indexPage = this.indexCollection
                    .getIndexStore(proc.namespace)
                    .getPageIndexPage(proc.pageNum);
                indexPage.setPageSubNum(proc.pageNum, proc.to);
                indexPage.save();
                tgtPage.create(srcPage.read());
            }
            if (srcPage.exists()) {
                // Duplicate, delete source.
                srcPage.delete();
            }
        }
        this.log.writeCheckpoint();
    }
}
