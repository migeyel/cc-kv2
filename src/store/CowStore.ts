import {
    IPage,
    IPageStore,
    IStoreCollection,
    Namespace,
    PageNum,
    PageSize,
} from "./IPageStore";
import { NAMESPACE_FMT, PAGE_FMT } from "../txStore/LogRecord/types";

const PAGE_KEY_FMT = NAMESPACE_FMT + PAGE_FMT;

export type CowHandler = (namespace: Namespace, pageNum: PageNum, data: string) => void;

class CowState {
    private handler?: CowHandler;
    private remaining = new LuaSet<string>();

    public setHandler(
        collection: IStoreCollection<IPage, IPageStore<IPage>>,
        handler: CowHandler,
    ) {
        this.handler = handler;
        this.remaining = new LuaSet();
        for (const namespace of collection.listStores()) {
            for (const pageNum of collection.getStore(namespace).listPages()) {
                this.remaining.add(string.pack(PAGE_KEY_FMT, namespace, pageNum));
            }
        }
    }

    public getRemainingPage(): LuaMultiReturn<[Namespace, PageNum] | []> {
        const key: string | undefined = next(this.remaining)[0];
        if (!key) { return $multi(); }
        const [namespace, pageNum] = string.unpack(PAGE_KEY_FMT, key);
        return $multi(namespace, pageNum);
    }

    public handlePageWrite(page: IPage) {
        const key = string.pack(PAGE_KEY_FMT, page.namespace, page.pageNum);
        if (this.remaining.has(key)) {
            assert(this.handler)(page.namespace, page.pageNum, assert(page.read()));
            this.remaining.delete(key);
        }
    }
}

/**
 * A CowCollection lets you attach a handler that gets called every time a page is
 * written to. This gives
 */
export class CowCollection implements IStoreCollection<CowPage, CowStore> {
    public readonly pageSize: PageSize;

    private inner: IStoreCollection<IPage, IPageStore<IPage>>;
    private state: CowState;

    public constructor(inner: IStoreCollection<IPage, IPageStore<IPage>>) {
        this.state = new CowState();
        this.inner = inner;
        this.pageSize = inner.pageSize;
    }

    /** Sets a handler that gets called on page writes. */
    public setHandler(handler: CowHandler) {
        this.state.setHandler(this.inner, handler);
    }

    /** Calls the handler on a page that it hasn't seen yet, if any. */
    public handleSomePage() {
        const [namespace, pageNum] = this.state.getRemainingPage();
        if (!namespace) { return; }
        this.state.handlePageWrite(this.getStore(namespace).getPage(pageNum));
    }

    public getStore(namespace: Namespace): CowStore {
        return new CowStore(this.state, this.inner.getStore(namespace));
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

    public constructor(state: CowState, inner: IPageStore<IPage>) {
        this.state = state;
        this.inner = inner;
        this.pageSize = inner.pageSize;
        this.namespace = inner.namespace;
    }

    public getPage(pageNum: PageNum): CowPage {
        return new CowPage(this.state, this.inner.getPage(pageNum));
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

    public constructor(state: CowState, inner: IPage) {
        this.state = state;
        this.inner = inner;
        this.pageSize = inner.pageSize;
        this.namespace = inner.namespace;
        this.pageNum = inner.pageNum;
    }

    public exists(): boolean {
        return this.inner.exists();
    }

    public create(initialData?: string): void {
        return this.inner.create(initialData);
    }

    public createOpen(): void {
        return this.inner.createOpen();
    }

    public delete(): void {
        this.state.handlePageWrite(this.inner);
        return this.inner.delete();
    }

    public read(): string | undefined {
        return this.inner.read();
    }

    public write(data: string): void {
        this.state.handlePageWrite(this.inner);
        return this.inner.write(data);
    }

    public append(extra: string): void {
        this.state.handlePageWrite(this.inner);
        return this.inner.append(extra);
    }

    public canAppend(): boolean {
        return this.inner.canAppend();
    }

    public openAppend(): void {
        return this.inner.openAppend();
    }

    public closeAppend(): void {
        return this.inner.closeAppend();
    }
}
