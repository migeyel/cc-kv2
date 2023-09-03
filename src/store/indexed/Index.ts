import { ObjCache } from "../../ObjCache";
import { IPage, IPageStore, IStoreCollection } from "../IPageStore";

/** The maximum number of substores the index can refer to. */
const MAX_INDEXED_SUBSTORES = 65535;

/** How many bytes each index entry occupies. */
const B_PER_ENTRY = math.ceil(math.log(1 + MAX_INDEXED_SUBSTORES, 256));

const ENTRY_FMT = string.format("<I%d", B_PER_ENTRY);

export class IndexCollection {
    private collection: IStoreCollection<IPage, IPageStore<IPage>>;

    private nullStr: string;

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
                this.nullStr,
            );
        };

        this.nullStr = string.rep("\0", collection.pageSize);
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

    private nullStr: string;

    // We need to share pages because they reflect global disk state (contents
    // of the index page).
    private pages: ObjCache<number, IndexPage>;

    public constructor(
        cacheSize: number,
        store: IPageStore<IPage>,
        nullStr: string,
    ) {
        const getter = (pageNum: number) => {
            return new IndexPage(
                this.store.getPage(pageNum),
                this.nullStr,
            );
        };

        this.store = store;
        this.nullStr = nullStr;
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

    private str: string;

    private nullStr: string;

    public constructor(page: IPage, nullStr: string) {
        this.nullStr = nullStr;
        this.str = page.read() || nullStr;
        this.page = page;
    }

    public save() {
        if (this.isEmpty()) {
            this.page.delete();
        } else {
            this.page.write(this.str);
        }
    }

    public isEmpty() {
        return this.str == this.nullStr;
    }

    public setPageSubNum(pageNum: number, subNum: number) {
        const entriesPerPage = math.floor(this.page.pageSize / B_PER_ENTRY);
        const rem = pageNum % entriesPerPage;
        const remByte = rem * B_PER_ENTRY;
        const prefix = string.sub(this.str, 0, remByte);
        const suffix = string.sub(this.str, remByte + B_PER_ENTRY + 1);
        this.str = prefix + (string.pack(ENTRY_FMT, subNum) + suffix);
    }

    public delPageSubNum(pageNum: number) {
        return this.setPageSubNum(pageNum, 0);
    }

    public getPageSubNum(pageNum: number): number | undefined {
        const entriesPerPage = math.floor(this.page.pageSize / B_PER_ENTRY);
        const rem = pageNum % entriesPerPage;
        assert((pageNum - rem) / entriesPerPage == this.page.pageNum);
        const [out] = string.unpack(ENTRY_FMT, this.str, rem * B_PER_ENTRY + 1);
        if (out != 0) { return out; }
    }
}
