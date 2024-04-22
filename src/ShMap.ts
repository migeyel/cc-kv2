import { Namespace, PageNum } from "./store/IPageStore";
import { NAMESPACE_FMT, PAGE_FMT } from "./txStore/LogRecord/types";

const PAGE_KEY_FMT = NAMESPACE_FMT + PAGE_FMT;

/** A map for storing shared stores and pages. */
export class ShMap<P, S> {
    private stores = setmetatable(new LuaMap<number, S>(), { __mode: "v" });
    private pages = setmetatable(new LuaMap<string, P>(), { __mode: "v" });

    public getStore(namespace: Namespace, fn: () => S): S {
        const out = this.stores.get(namespace);
        if (out) { return out; }
        const newOut = fn();
        this.stores.set(namespace, newOut);
        return newOut;
    }

    public getPage(namespace: Namespace, pageNum: PageNum, fn: () => P): P {
        const key = string.pack(PAGE_KEY_FMT, namespace, pageNum);
        const out = this.pages.get(key);
        if (out) { return out; }
        const newOut = fn();
        this.pages.set(key, newOut);
        return newOut;
    }
}
