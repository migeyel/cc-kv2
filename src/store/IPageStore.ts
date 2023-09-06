/** The largest length a namespace string can take. */
export const MAX_NAMESPACE_LEN = 32;

/** The maximum page number a store can hold. */
export const MAX_PAGE_NUM = 2 ** 48 - 1;

/** A named collection of pages in a store. */
export type Namespace = string & { readonly __brand: unique symbol };

/** A page size. */
export type PageSize = number & { readonly __brand: unique symbol };

/** A page number. */
export type PageNum = number & { readonly __brand: unique symbol };

/**
 * A (possibly non-existent) page in a page store.
 *
 * A page is a container for a data string, indexed by a nonnegative page
 * number. Pages can handle any sized strings but have a strongly preferred
 * maximum size for strings on them.
 *
 * Pages tend to map 1:1 with a file in the disk and can be very efficiently
 * appended by keeping a file handle open.
 */
export interface IPage {
    /** The preferential maximum size for this page. */
    readonly pageSize: PageSize;

    /** The page number for this page. */
    readonly pageNum: PageNum;

    /** Whether the page exists or not. */
    exists(): boolean;

    /** Creates the page in the store. */
    create(initialData?: string): void;

    /** Creates the page and opens for append. */
    createOpen(): void;

    /** Deletes the page from the store. */
    delete(): void;

    /** Reads from the page. Returns nothing if it doesn't exist. */
    read(): string | undefined;

    /** Writes to the page. */
    write(data: string): void;

    /** Appends data to the page. */
    append(extra: string): void;

    /** Whether this page is open for appending. */
    canAppend(): boolean;

    /** Opens the page for appending. */
    openAppend(): void;

    /** Closes the page for appending. */
    closeAppend(): void;

    /** Flushes this page to disk. */
    flush(): void;
}

/**
 * A generic store for disk pages.
 */
export interface IPageStore<P extends IPage> {
    /** The preferential maximum page size for pages in the store. */
    readonly pageSize: PageSize;

    /** Specifies a page in the store. */
    getPage(pageNum: PageNum): P;

    /** Lists all pages in the store. */
    listPages(): LuaSet<PageNum>;
}

/**
 * A store collection unites several page stores under string namespaces.
 */
export interface IStoreCollection<P extends IPage, S extends IPageStore<P>> {
    /** The preferential maximum page size for pages in the collection. */
    readonly pageSize: PageSize;

    /** Specifies a store in the collection. */
    getStore(namespace: Namespace): S;

    /** Lists all stores with at least one page in the collection. */
    listStores(): LuaSet<Namespace>;
}
