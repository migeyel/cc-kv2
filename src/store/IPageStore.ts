/** A serializable object. */
export interface ISerializable {
    serialize(): string,
}

/** A deserializer for an object. */
export type Deserializer<T extends ISerializable> = {
    deserialize(this: void, serialized: string): T,
}

/** A type that can be updated by appending to serialized data. */
export interface IAppendableWith<A extends ISerializable>
    extends ISerializable
{
    appendUpdate(extra: A): this,
}

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
export interface IPage<T extends IAppendableWith<A>, A extends ISerializable> {
    /** The preferential maximum size for this page. */
    readonly pageSize: number;

    /** Whether the page exists or not. */
    exists(): boolean;

    /** Creates the page in the store. */
    create(initialData: string): void;

    /** Creates the page and opens for append. */
    createOpen(): void;

    /** Deletes the page from the store. */
    delete(): void;

    /** Reads from the page. Returns nothing if it doesn't exist. */
    read(): T | undefined;

    /** Writes to the page. */
    write(data: T): void;

    /** Appends data to the page. */
    append(extra: A): void;

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
export interface IPageStore<
    T extends IAppendableWith<A>,
    A extends ISerializable,
    P extends IPage<T, A>
> {
    /** The preferential maximum page size for pages in the store. */
    readonly pageSize: number;

    /** Specifies a page in the store. */
    getPage(pageNum: number): P;
}

/**
 * A store collection unites several page stores under string namespaces.
 */
export interface IStoreCollection<
    T extends IAppendableWith<A>,
    A extends ISerializable,
    P extends IPage<T, A>,
    S extends IPageStore<T, A, P>
> {
    /** The preferential maximum page size for pages in the collection. */
    readonly pageSize: number;

    /** Specifies a store in the collection. */
    getStore(namespace: string): S;
}
