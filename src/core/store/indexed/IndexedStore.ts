import { BTreeComponent } from "../../btree/Node";
import { CacheMap } from "../../CacheMap";
import { ConfigEntryComponent } from "../../ConfigPageComponent";
import { PageAllocatorComponent } from "../../PageAllocatorComponent";
import { ShMap } from "../../ShMap";
import { RecordLog } from "../../RecordLog";
import { RecordsComponent } from "../../records/Records";
import { SetEntryAct, SetEntryConfig } from "../../SetEntryConfig";
import { NAMESPACE_FMT, PAGE_FMT } from "../../txStore/LogRecord/types";
import { TxCollection, TxId } from "../../txStore/LogStore";
import {
    IPage,
    IPageStore,
    IStoreCollection,
    Namespace,
    PageNum,
    PageSize,
} from "../IPageStore";
import { SUBSTORE_FMT, SUBSTORE_LFMT } from "./IndexObj";

/** A number identifying a substore. Must not be 0. */
type SubstoreNum = number & { readonly __brand: unique symbol };

enum Namespaces {
    LOG,
    CONFIG,
    HEADERS,
    PAGES,
    LEAVES,
    BRANCHES,
}

enum ConfigKeys {
    RECORDS_ALLOCATOR_NUM_PAGES,
    LEAVES_ALLOCATOR_NUM_PAGES,
    BRANCHES_ALLOCATOR_NUM_PAGES,
    BTREE_ROOT,
}

type SubstoreLoader = (
    description: string
) => IStoreCollection<IPage, IPageStore<IPage>>;

export class IndexedCollection implements IStoreCollection<IndexedPage, IndexedStore> {
    public readonly pageSize: PageSize;

    private state: IndexState;

    public constructor(
        pageSize: PageSize,
        indexCollection: IStoreCollection<IPage, IPageStore<IPage>>,
        loader: SubstoreLoader,
    ) {
        this.pageSize = pageSize;
        this.state = new IndexState(indexCollection, loader);
    }

    public close(): void {
        return this.state.close();
    }

    public getStore(namespace: Namespace): IndexedStore {
        return this.state.map.getStore(namespace, () => new IndexedStore(
            this.state,
            this.pageSize,
            namespace,
        ));
    }

    public listStores(): LuaSet<Namespace> {
        return this.state.stores();
    }

    public getSubstore(desc: string): Substore | undefined {
        const substoreNum = this.state.invSubstores.get(desc);
        if (!substoreNum) { return; }
        return assert(this.state.substores.get(substoreNum));
    }

    public addSubstore(desc: string, quota: number) {
        const substoreNum = this.state.findUnusedSubstoreNum();
        this.state.addSubstore(substoreNum, this.state.loader(desc), desc, quota);
    }

    public delSubstore(desc: string) {
        const substoreNum = assert(this.state.invSubstores.get(desc));
        this.state.requotaSubstore(substoreNum, 0);
        this.state.delSubstore(substoreNum);
    }

    public setSubstoreQuota(desc: string, quota: number) {
        const substoreNum = assert(this.state.invSubstores.get(desc));
        this.state.requotaSubstore(substoreNum, quota);
    }

    public getUsage(): number {
        return this.state.totalUsage;
    }

    public getQuota(): number {
        return this.state.totalQuota;
    }

    public getConfig(key: string): string | undefined {
        return this.state.getConfig(key);
    }

    public setConfig(key: string, value?: string): void {
        return this.state.setConfig(key, value);
    }
}

class IndexedStore implements IPageStore<IndexedPage> {
    public readonly pageSize: PageSize;
    public readonly namespace: Namespace;

    private state: IndexState;

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
        return this.state.map.getPage(this.namespace, pageNum, () => new IndexedPage(
            this.state,
            this.pageSize,
            this.namespace,
            pageNum,
        ));
    }

    public listPages(): LuaSet<PageNum> {
        return this.state.pages(this.namespace);
    }
}

class IndexedPage implements IPage {
    public readonly pageSize: PageSize;
    public readonly namespace: Namespace;
    public readonly pageNum: PageNum;

    private state: IndexState;

    /** Current substore page, if allocated. */
    public page?: IPage;

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
        const substoreNum = this.state.getPageSubstoreNum(namespace, pageNum);
        if (substoreNum) {
            this.page = assert(this.state.substores.get(substoreNum))
                .collection
                .getStore(namespace)
                .getPage(pageNum);
        }
    }

    public exists(): boolean {
        if (!this.page) { return false; }
        return assert(this.page.exists());
    }

    public create(initialData?: string | undefined): void {
        assert(!this.page);
        const substoreNum = this.state.findAvailableStore();
        const substore = assert(this.state.substores.get(substoreNum));
        const page = substore.collection
            .getStore(this.namespace)
            .getPage(this.pageNum);

        this.state.markDel(substoreNum, this.namespace, this.pageNum);
        page.create(initialData);
        this.state.allocatePage(substoreNum, this.namespace, this.pageNum);

        this.page = page;
    }

    public move(substoreNum: SubstoreNum): void {
        this.state.movePage(this.namespace, this.pageNum, substoreNum);
    }

    public createOpen(): void {
        assert(!this.page);
        const substoreNum = this.state.findAvailableStore();
        const substore = assert(this.state.substores.get(substoreNum));
        const page = substore.collection
            .getStore(this.namespace)
            .getPage(this.pageNum);

        this.state.markDel(substoreNum, this.namespace, this.pageNum);
        page.createOpen();
        this.state.allocatePage(substoreNum, this.namespace, this.pageNum);

        this.page = page;
    }

    public delete(): void {
        assert(this.page);
        this.state.freePage(this.namespace, this.pageNum);
        this.page = undefined;
    }

    public read(): string | undefined {
        return this.page?.read();
    }

    public write(data: string): void {
        return assert(this.page).write(data);
    }

    public append(extra: string): void {
        return assert(this.page).append(extra);
    }

    public canAppend(): boolean {
        return !!this.page && this.page.canAppend();
    }

    public openAppend(): void {
        return assert(this.page).openAppend();
    }

    public closeAppend(): void {
        return assert(this.page).closeAppend();
    }
}

/** Information for a substore. */
type Substore = {
    /** The collection storing pages. */
    collection: IStoreCollection<IPage, IPageStore<IPage>>,

    /** A string describing how to find the collection on disk. */
    desc: string,

    /** Maximum usable amount of pages. */
    quota: number,

    /** How many pages have been allocated. */
    usage: number,
}

/** Prefix for keys used in the configuration db. */
enum Prefix {
    /** Indexes pages into substore numbers. */
    INDEX = 0,

    /** Stores the desc + quota fields. */
    DESC = 1,

    /** Stores the usage field. */
    USAGE = 2,

    /** Asks for a page to be deleted on recovery if the index doesn't point to it. */
    DEL = 3,

    /** Stores other configuration. */
    CONFIG = 4,
}

const INDEX_KEY_FMT = ">B" + NAMESPACE_FMT + PAGE_FMT;
const INDEX_VAL_FMT = SUBSTORE_FMT;

const DESC_KEY_FMT = ">B" + SUBSTORE_FMT;
const DESC_VAL_FMT = "<s2" + PAGE_FMT;

const USAGE_KEY_FMT = ">B" + SUBSTORE_LFMT;
const USAGE_VAL_FMT = "<" + PAGE_FMT;

const DEL_KEY = string.char(Prefix.DEL);
const DEL_VAL_FMT = "<" + SUBSTORE_FMT + NAMESPACE_FMT + PAGE_FMT;

/**
 * Maintains an index mapping pages in a virtual store to pages in one or more physical
 * stores.
 */
class IndexState {
    public cl: TxCollection;
    public config: SetEntryConfig;
    public log: RecordLog;
    public substores: LuaMap<SubstoreNum, Substore>;
    public invSubstores: LuaMap<string, SubstoreNum>;
    public nonFullSubstores: LuaSet<SubstoreNum>;
    public loader: SubstoreLoader;
    public totalQuota: number;
    public totalUsage: number;
    public map = new ShMap<IndexedPage, IndexedStore>();

    public constructor(
        indexCollection: IStoreCollection<IPage, IPageStore<IPage>>,
        loader: SubstoreLoader,
    ) {
        const btree = new BTreeComponent(
            indexCollection,
            new RecordsComponent(
                indexCollection,
                new PageAllocatorComponent(
                    new ConfigEntryComponent(
                            Namespaces.CONFIG as Namespace,
                            ConfigKeys.RECORDS_ALLOCATOR_NUM_PAGES,
                    ),
                    Namespaces.PAGES as Namespace,
                ),
                Namespaces.HEADERS as Namespace,
            ),
            new ConfigEntryComponent(
                Namespaces.CONFIG as Namespace,
                ConfigKeys.BTREE_ROOT as Namespace,
            ),
            new PageAllocatorComponent(
                new ConfigEntryComponent(
                    Namespaces.CONFIG as Namespace,
                    ConfigKeys.LEAVES_ALLOCATOR_NUM_PAGES,
                ),
                Namespaces.LEAVES as Namespace,
            ),
            new PageAllocatorComponent(
                new ConfigEntryComponent(
                    Namespaces.CONFIG as Namespace,
                    ConfigKeys.BRANCHES_ALLOCATOR_NUM_PAGES,
                ),
                Namespaces.BRANCHES as Namespace,
            ),
        );

        // Instantiate the config db.
        this.log = new RecordLog(indexCollection.getStore(Namespaces.LOG as Namespace));
        this.config = new SetEntryConfig(new CacheMap(32), btree);
        this.cl = new TxCollection(this.log, indexCollection, this.config, 8, 32);

        // Load substores.
        this.loader = loader;
        this.nonFullSubstores = new LuaSet();
        this.substores = new LuaMap();
        this.invSubstores = new LuaMap();
        this.totalQuota = 0;
        this.totalUsage = 0;
        let descKey = string.char(Prefix.DESC);
        while (true) {
            const [_, descKv] = this.config.btree.search(this.cl, descKey + "\0");
            if (!descKv) { break; }
            if (string.byte(descKv.key) != Prefix.DESC) { break; }

            descKey = descKv.key;
            const [__, substoreNum] = string.unpack(DESC_KEY_FMT, descKey);
            const [desc, quota] = string.unpack(DESC_VAL_FMT, descKv.value);

            const usageKey = string.pack(USAGE_KEY_FMT, Prefix.USAGE, substoreNum);
            const [___, usageKv] = this.config.btree.search(this.cl, usageKey);
            assert(assert(usageKv).key == usageKey);
            const [usage] = string.unpack(USAGE_VAL_FMT, usageKv!.value);

            this.totalQuota += quota;
            this.totalUsage += usage;

            this.substores.set(substoreNum, {
                collection: loader(desc),
                desc,
                quota,
                usage,
            });

            this.invSubstores.set(desc, substoreNum);

            if (usage < quota) { this.nonFullSubstores.add(substoreNum); }
        }

        // Honor DEL record.
        {
            const [_, kv] = this.config.btree.search(this.cl, DEL_KEY);
            if (kv?.key == DEL_KEY) {
                const [substoreNum, namespace, pageNum] = string.unpack(
                    DEL_VAL_FMT,
                    kv.value,
                );
                const newSubstoreNum = this.getPageSubstoreNum(namespace, pageNum);

                if (substoreNum != newSubstoreNum) {
                    const substore = assert(this.substores.get(substoreNum));
                    const page = substore.collection
                        .getStore(namespace)
                        .getPage(pageNum);
                    if (page.exists()) { page.delete(); }
                }
            }
        }
    }

    public stores(): LuaSet<Namespace> {
        let curNamespace = 0;
        const out = new LuaSet<Namespace>();
        while (true) {
            const key = string.pack(INDEX_KEY_FMT, Prefix.INDEX, curNamespace, 0);
            const [_, kv] = this.config.btree.search(this.cl, key);
            if (!kv || string.byte(kv.key) != Prefix.INDEX) { return out; }
            const [__, nextNamespace] = string.unpack(INDEX_KEY_FMT, kv.key);
            out.add(nextNamespace);
            curNamespace = nextNamespace + 1;
        }
    }

    public pages(namespace: Namespace): LuaSet<PageNum> {
        let curPage = 0;
        const out = new LuaSet<PageNum>();
        while (true) {
            const key = string.pack(INDEX_KEY_FMT, Prefix.INDEX, namespace, curPage);
            const [_, kv] = this.config.btree.search(this.cl, key);
            if (!kv || string.byte(kv.key) != Prefix.INDEX) { return out; }
            const [__, nextNamespace, nextPage] = string.unpack(INDEX_KEY_FMT, kv.key);
            if (nextNamespace != namespace) { return out; }
            out.add(nextPage);
            curPage = nextPage + 1;
        }
    }

    public findUnusedSubstoreNum(): SubstoreNum {
        if (this.substores.isEmpty()) { return 0 as SubstoreNum; }
        const ceiling = (this.substores as unknown as { l: LuaLengthMethod<any> }).l();
        const attempt = math.random(1, ceiling + 1) as SubstoreNum;
        if (!this.substores.has(attempt)) { return attempt; }
        return (ceiling + 1) as SubstoreNum;
    }

    /** Gets the substore number used by a page, if it is in the index. */
    public getPageSubstoreNum(
        namespace: Namespace,
        pageNum: PageNum,
    ): SubstoreNum | undefined {
        const key = string.pack(INDEX_KEY_FMT, Prefix.INDEX, namespace, pageNum);
        const [_, kv] = this.config.btree.search(this.cl, key);
        if (!kv || kv.key !== key) { return; }
        return string.unpack(INDEX_VAL_FMT, kv.value)[0];
    }

    /** Marks a page with the DEL record. */
    public markDel(substoreNum: SubstoreNum, namespace: Namespace, pageNum: PageNum) {
        const value = string.pack(DEL_VAL_FMT, substoreNum, namespace, pageNum);
        this.cl.doAct(0 as TxId, <SetEntryAct>{ key: DEL_KEY, value });
        this.cl.commit(0 as TxId);
    }

    private setUsage(txId: TxId, substoreNum: SubstoreNum, usage: number) {
        const substore = assert(this.substores.get(substoreNum));
        assert(usage <= substore.quota, "can't raise usage to above quota");

        const usageKey = string.pack(USAGE_KEY_FMT, Prefix.USAGE, substoreNum);
        const usageValue = string.pack(USAGE_VAL_FMT, usage);
        this.cl.doAct(txId, <SetEntryAct>{ key: usageKey, value: usageValue });

        this.totalUsage = this.totalUsage - substore.usage + usage;
        substore.usage = usage;
        if (substore.usage < substore.quota) {
            this.nonFullSubstores.add(substoreNum);
        } else {
            this.nonFullSubstores.delete(substoreNum);
        }
    }

    /**
     * Returns a non-full substore.
     * @throws If all substores are full.
     */
    public findAvailableStore(): SubstoreNum {
        return assert(next(this.nonFullSubstores)[0], "out of space")[0];
    }

    /**
     * Adds a page to the index.
     * @throws If the page is already in the index.
     * @throws If the substore is full.
     */
    public allocatePage(
        substoreNum: SubstoreNum,
        namespace: Namespace,
        pageNum: PageNum,
    ): LuaMultiReturn<[SubstoreNum, Substore]> {
        assert(!this.getPageSubstoreNum(namespace, pageNum));
        const substore = assert(this.substores.get(substoreNum));
        assert(substore.usage < substore.quota, "can't allocate above quota");

        const key = string.pack(INDEX_KEY_FMT, Prefix.INDEX, namespace, pageNum);
        const value = string.pack(INDEX_VAL_FMT, substoreNum);
        this.cl.doAct(0 as TxId, <SetEntryAct>{ key, value });
        this.setUsage(0 as TxId, substoreNum, substore.usage + 1);
        this.cl.commit(0 as TxId);
        return $multi(substoreNum, substore);
    }

    /**
     * Removes a page from the index and deletes its contents.
     * @throws If the page isn't in the index.
     */
    public freePage(namespace: Namespace, pageNum: PageNum) {
        const substoreNum = assert(this.getPageSubstoreNum(namespace, pageNum));
        const substore = assert(this.substores.get(substoreNum));
        const page = substore.collection
            .getStore(namespace)
            .getPage(pageNum);

        this.markDel(substoreNum, namespace, pageNum);

        const key = string.pack(INDEX_KEY_FMT, Prefix.INDEX, namespace, pageNum);
        this.cl.doAct(0 as TxId, <SetEntryAct>{ key });
        this.setUsage(0 as TxId, substoreNum, substore.usage - 1);
        this.cl.commit(0 as TxId);

        page.delete();
    }

    /**
     * Moves a page from one substore to another. This is a managed operation that also
     * ensures the data is copied without error.
     * @throws If the page is already at the target substore.
     * @throws If the page isn't in the index.
     * @throws If the target substore is full.
     */
    public movePage(
        namespace: Namespace,
        pageNum: PageNum,
        tgtSubstoreNum: SubstoreNum,
    ) {
        const srcSubstoreNum = assert(this.getPageSubstoreNum(namespace, pageNum));
        assert(srcSubstoreNum != tgtSubstoreNum);
        const srcSubstore = assert(this.substores.get(srcSubstoreNum));
        const tgtSubstore = assert(this.substores.get(tgtSubstoreNum));
        assert(tgtSubstore.usage < tgtSubstore.quota, "can't move above quota");

        const srcPage = srcSubstore.collection
            .getStore(namespace)
            .getPage(pageNum);
        const tgtPage = tgtSubstore.collection
            .getStore(namespace)
            .getPage(pageNum);

        // Set the DEL record to delete the new copy.
        this.markDel(tgtSubstoreNum, namespace, pageNum);

        // Copy the contents over.
        const data = srcPage.read();
        tgtPage.create(data);

        // Perform the move and set the DEL record to the old copy.
        const indexKey = string.pack(INDEX_KEY_FMT, Prefix.INDEX, namespace, pageNum);
        const indexVal = string.pack(INDEX_VAL_FMT, tgtSubstoreNum);
        const delVal = string.pack(DEL_VAL_FMT, srcSubstoreNum, namespace, pageNum);
        this.cl.doAct(0 as TxId, <SetEntryAct>{ key: indexKey, value: indexVal });
        this.cl.doAct(0 as TxId, <SetEntryAct>{ key: DEL_KEY, value: delVal });
        this.setUsage(0 as TxId, srcSubstoreNum, srcSubstore.usage - 1);
        this.setUsage(0 as TxId, tgtSubstoreNum, tgtSubstore.usage + 1);
        this.cl.commit(0 as TxId);

        // Delete the old copy.
        if (srcPage.canAppend()) { tgtPage.openAppend(); }
        srcPage.delete();
        const page = this.map.tryGetPage(namespace, pageNum);
        if (page) { page.page = tgtPage; }
    }

    /**
     * Moves pages away from a substore to meet a target.
     * @throws If the other substores don't have enough space.
     */
    private drainSubstore(substoreNum: SubstoreNum, target: number) {
        const substore = assert(this.substores.get(substoreNum));
        for (const namespace of substore.collection.listStores()) {
            const store = substore.collection.getStore(namespace);
            for (const pageNum of store.listPages()) {
                if (substore.usage <= target) { return; }
                let tgtSubstoreNum = next(this.nonFullSubstores)[0];
                assert(tgtSubstoreNum, "out of space");
                if (tgtSubstoreNum == substoreNum) {
                    tgtSubstoreNum = next(this.nonFullSubstores, tgtSubstoreNum)[0];
                    assert(tgtSubstoreNum, "out of space");
                }
                this.movePage(namespace, pageNum, tgtSubstoreNum);
            }
        }
    }

    /**
     * Adds a substore to the index.
     * @throws If the substore number is already being used.
     * @throws If the substore description is already being used.
     */
    public addSubstore(
        substoreNum: SubstoreNum,
        collection: IStoreCollection<IPage, IPageStore<IPage>>,
        desc: string,
        quota: number,
    ) {
        assert(quota >= 0);
        assert(!this.substores.has(substoreNum));
        assert(!this.invSubstores.has(desc));

        const descKey = string.pack(DESC_KEY_FMT, Prefix.DESC, substoreNum);
        const usageKey = string.pack(USAGE_KEY_FMT, Prefix.USAGE, substoreNum);
        const descValue = string.pack(DESC_VAL_FMT, desc, quota);
        const usageValue = string.pack(USAGE_VAL_FMT, 0);
        this.cl.doAct(0 as TxId, <SetEntryAct>{ key: descKey, value: descValue });
        this.cl.doAct(0 as TxId, <SetEntryAct>{ key: usageKey, value: usageValue });
        this.cl.commit(0 as TxId);

        this.substores.set(substoreNum, {
            collection,
            quota,
            desc,
            usage: 0,
        });

        this.invSubstores.set(desc, substoreNum);
        this.totalQuota += quota;

        if (0 < quota) { this.nonFullSubstores.add(substoreNum); }
    }

    /**
     * Deletes a substore from the index.
     * @throws If the substore isn't empty.
     */
    public delSubstore(substoreNum: SubstoreNum) {
        const substore = assert(this.substores.get(substoreNum));
        assert(substore.usage == 0, "can't delete nonempty substore");

        const descKey = string.pack(DESC_KEY_FMT, Prefix.DESC, substoreNum);
        const usageKey = string.pack(USAGE_KEY_FMT, Prefix.USAGE, substoreNum);
        this.cl.doAct(0 as TxId, <SetEntryAct>{ key: descKey });
        this.cl.doAct(0 as TxId, <SetEntryAct>{ key: usageKey });
        this.cl.doAct(0 as TxId, <SetEntryAct>{ key: DEL_KEY });
        this.cl.commit(0 as TxId);

        this.substores.delete(substoreNum);
        this.invSubstores.delete(substore.desc);
        this.nonFullSubstores.delete(substoreNum);
        this.totalQuota -= substore.quota;
    }

    /**
     * Changes a substore's maximum page quota. Existing pages are moved to other
     * substores if needed.
     * @throws If the other substores can't hold enough pages.
     */
    public requotaSubstore(substoreNum: SubstoreNum, quota: number) {
        const substore = assert(this.substores.get(substoreNum));
        if (substore.usage > quota) { this.drainSubstore(substoreNum, quota); }

        const descKey = string.pack(DESC_KEY_FMT, Prefix.DESC, substoreNum);
        const descValue = string.pack(DESC_VAL_FMT, substore.desc, quota);
        this.cl.doAct(0 as TxId, <SetEntryAct>{ key: descKey, value: descValue });
        this.cl.commit(0 as TxId);

        this.totalQuota = this.totalQuota - substore.quota + quota;
        substore.quota = quota;
    }

    /** Gets a config from the index DB. */
    public getConfig(key: string): string | undefined {
        key = string.char(Prefix.CONFIG) + key;
        const [_, v] = this.config.btree.search(this.cl, key);
        if (v?.key != key) { return; }
        return v.value;
    }

    /** Sets a config to the index DB. */
    public setConfig(key: string, value?: string): void {
        key = string.char(Prefix.CONFIG) + key;
        this.cl.doAct(0 as TxId, <SetEntryAct>{ key, value });
        this.cl.commit(0 as TxId);
    }

    /** Flushes and closes the log. */
    public close(): void {
        return this.log.close();
    }
}
