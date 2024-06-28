import {
    IPage,
    IPageStore,
    IStoreCollection,
    Namespace,
    PageSize,
} from "./IPageStore";

/**
 * A collection that maps over namespaces.
 *
 * MappedCollection gathers a finite set of stores from other collections and remaps
 * their namespaces into itself. This lets you join several collections' namespaces
 * together, or split a collection into several other ones.
 */
export class MappedCollection implements IStoreCollection<IPage, IPageStore<IPage>> {
    public pageSize: PageSize;

    private stores: LuaMap<Namespace, IPageStore<IPage>>;

    public constructor(stores: LuaMap<Namespace, IPageStore<IPage>>) {
        this.pageSize = (assert(next(stores)[1]) as IPageStore<IPage>).pageSize;
        for (const [_, store] of stores) { assert(store.pageSize == this.pageSize); }
        this.stores = stores;
    }

    public getStore(namespace: Namespace): IPageStore<IPage> {
        return assert(this.stores.get(namespace));
    }

    public listStores(): LuaSet<Namespace> {
        const out = new LuaSet<Namespace>();
        for (const [namespace, store] of this.stores) {
            if (!store.listPages().isEmpty()) { out.add(namespace); }
        }
        return out;
    }
}
