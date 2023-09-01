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
    /** Whether the page exists or not. */
    exists(): boolean;

    /** Creates the page in the store. */
    create(initialData: string): void;

    /** Creates the page and opens for append. */
    createOpen(): void;

    /** Deletes the page from the store. */
    delete(): void;

    /** Reads from the page. Returns nothing if it doesn't exist. */
    read(): string | undefined;

    /** Writes to the page. */
    write(data: string): void;

    /** Appends data to the page. */
    append(data: string): void;

    /** Whether this page is open for appending. */
    canAppend(): boolean;

    /** Opens the page for appending. */
    openAppend(): void;

    /** Opens the page for appending, and truncates its contents. */
    openAppendTruncate(): void;

    /** Closes the page for appending. */
    closeAppend(): void;

    /** Flushes this page to disk. */
    flush(): void;
}

/**
 * A generic store for disk pages.
 */
export interface IPageStore<P extends IPage> {
    /** Fetches a page from the store. */
    getPage(pageNum: number): P;
}

/**
 * A store collection unites several page stores under string namespaces.
 */
export interface IStoreCollection<P extends IPage, S extends IPageStore<P>> {
    getStore(namespace: string): S;
}
