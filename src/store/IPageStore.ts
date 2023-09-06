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
 * number. Pages have a set maximum page size, and may throw errors if a larger
 * value is written, depending on implementation.
 *
 * ## Pages are Shared
 * Because they are meant to reflect global disk state, implementors must ensure
 * that all references to the same page are always shared. Equality between
 * pages can also be checked by checking if they are the same object.
 *
 * ## Writes may not Persist until Flushed
 * Implementors may choose to not persist writes until later, or until `flush()`
 * is called. If a page has no disk backing it may never persist.
 */
export interface IPage {
    /** The maximum size for contents in this page. */
    readonly pageSize: PageSize;

    /** The page number for this page. */
    readonly pageNum: PageNum;

    /** Whether the page exists or not. */
    exists(): boolean;

    /**
     * Creates the page in the store.
     * @throws If the page already exists and is open for appending.
     *
     * If the page already exists but isn't open for appending, implementors are
     * free to either throw or overwrite its contents with anything else.
     */
    create(initialData?: string): void;

    /**
     * Creates the page and opens for appending.
     * @throws If the page already exists and is open for appending.
     *
     * If the page already exists but isn't open for appending, implementors are
     * free to either throw or overwrite its contents with anything else.
     */
    createOpen(): void;

    /**
     * Deletes the page from the store.
     * @throws If the page is open for appending.
     *
     * If the page doesn't exist, implementors are free to throw or do nothing.
     */
    delete(): void;

    /** Reads from the page. Returns nothing if it doesn't exist. */
    read(): string | undefined;

    /**
     * Writes to the page.
     * @throws If the page is open for appending.
     *
     * If the page doesn't exist, implementors are free to throw or overwrite
     * their contents with anything else.
     */
    write(data: string): void;

    /**
     * Appends data to the page.
     * @throws If the page isn't open for appending.
     */
    append(extra: string): void;

    /** Whether this page is open for appending. */
    canAppend(): boolean;

    /**
     * Opens the page for appending.
     * @throws If the page is already open for appending, since that often means
     * a concurrent write attempt.
     *
     * If the page doesn't exist, implementors are free to throw or overwrite
     * their contents with anything else.
     */
    openAppend(): void;

    /**
     * Closes the page for appending.
     * @throws If the page is'nt open for appending, since that often means a
     * concurrent write attempt.
     */
    closeAppend(): void;

    /**
     * Flushes a page to disk.
     * @throws If the page doesn't exist.
     *
     * When this function returns, and if the page has a disk backing, ensures
     * that its contents will persist after a reboot.
     *
     * Flushing is atomic: the disk page's contents either contain the complete
     * contents of the flush, or the complete contents before it, but nothing
     * in between those. However, due to CC limitations, this may not apply in
     * the event of a host power failure, hard reboot, or kernel panic.
     */
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
