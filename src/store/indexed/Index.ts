import { ObjCache } from "../../ObjCache";
import { IPage, IPageStore, IStoreCollection } from "../IPageStore";

/** The maximum number of substores the index can refer to. */
const MAX_INDEXED_SUBSTORES = 65535;

/** How many bytes each index entry occupies. */
const B_PER_ENTRY = math.floor(math.log(MAX_INDEXED_SUBSTORES, 256));

export class IndexCollection {
    private collection: IStoreCollection<IPage, IPageStore<IPage>>;

    private helperFmt: string;

    // We need to share stores because we need to share pages.
    private stores: ObjCache<string, IndexStore>;

    public constructor(
        cacheSize: number,
        collection: IStoreCollection<IPage, IPageStore<IPage>>,
    ) {
        const getter = (namespace: string) => {
            return new IndexStore(
                cacheSize,
                this.collection.getStore(namespace),
                this.helperFmt,
            );
        };

        const entriesPerPage = math.floor(collection.pageSize / B_PER_ENTRY);
        this.helperFmt = "<" + string.rep("I" + B_PER_ENTRY, entriesPerPage);
        this.collection = collection;
        this.stores = new ObjCache(cacheSize, getter);
    }

    public getIndexStore(namespace: string): IndexStore {
        return this.stores.get(namespace);
    }
}

/** A store for index page objects. */
export class IndexStore {
    private store: IPageStore<IPage>;

    private helperFmt: string;

    // We need to share pages because they reflect global disk state (contents
    // of the index page).
    private pages: ObjCache<number, IndexPage>;

    public constructor(
        cacheSize: number,
        store: IPageStore<IPage>,
        helperFmt: string,
    ) {
        const getter = (pageNum: number) => {
            return new IndexPage(
                this.store.getPage(pageNum),
                this.helperFmt,
            );
        };

        this.store = store;
        this.helperFmt = helperFmt;
        this.pages = new ObjCache(cacheSize, getter);
    }

    public getPageIndexPage(dataPageNum: number): IndexPage {
        const entriesPerPage = math.floor(this.store.pageSize / B_PER_ENTRY);
        return this.pages.get(math.floor(dataPageNum / entriesPerPage));
    }
}

/** A stored page with index information on it. */
export class IndexPage {
    private page: IPage;

    /** The page entries. */
    private entries: number[];

    private helperFmt: string;

    public constructor(page: IPage, helperFmt: string) {
        const str = page.read() || string.rep("\0", page.pageSize);
        this.page = page;
        this.helperFmt = helperFmt;
        this.entries = string.unpack(helperFmt, str);
        this.entries.pop();
    }

    public save() {
        if (this.isEmpty()) {
            this.page.delete();
        } else {
            this.page.write(string.pack(this.helperFmt, ...this.entries));
        }
    }

    public isEmpty() {
        for (const entry of this.entries) { if (entry != 0) { return false; } }
        return true;
    }

    public setPageSubNum(pageNum: number, subNum: number) {
        const entriesPerPage = math.floor(this.page.pageSize / B_PER_ENTRY);
        const rem = pageNum % entriesPerPage;
        assert((pageNum - rem) / entriesPerPage == this.page.pageNum);
        this.entries[rem] = subNum;
    }

    public delPageSubNum(pageNum: number) {
        const entriesPerPage = math.floor(this.page.pageSize / B_PER_ENTRY);
        const rem = pageNum % entriesPerPage;
        assert((pageNum - rem) / entriesPerPage == this.page.pageNum);
        this.entries[rem] = 0;
    }

    public getPageSubNum(pageNum: number): number | undefined {
        const entriesPerPage = math.floor(this.page.pageSize / B_PER_ENTRY);
        const rem = pageNum % entriesPerPage;
        assert((pageNum - rem) / entriesPerPage == this.page.pageNum);
        const out = this.entries[rem];
        if (out != 0) { return out; }
    }
}
