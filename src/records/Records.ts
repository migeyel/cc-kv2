import { AllocatedPageComponent } from "../AllocatedPageComponent";
import {
    IPage,
    IPageStore,
    IStoreCollection,
    Namespace,
    PageNum,
    PageSize,
} from "../store/IPageStore";
import { IEvent, IObj, TxCollection, TxPage } from "../txStore/LogStore";
import {
    HeaderEvent,
    HeaderObj,
    deserializeHeaderEvent,
    deserializeHeaderObj,
} from "./Header";
import {
    NO_LINK,
    EntryEvent,
    RecordPageObj,
    ENTRY_OVERHEAD,
    CreateEntryEvent,
    SetLinksEvent,
    deserializeRecordPageObj,
    deserializeEntryEvent,
    DeleteEntryEvent,
    WriteEntryEvent,
    ENTRY_ID_BYTES,
} from "./RecordPage";
import {
    PAGE_LINK_BYTES,
    SizeClass,
    getClassThatFits,
    getSizeClass,
} from "./SizeClass";

/** The size of a serialized record ID, in bytes. */
export const RECORD_ID_BYTES = PAGE_LINK_BYTES + ENTRY_ID_BYTES;

const RECORD_ID_FMT = "<I" + PAGE_LINK_BYTES + "I" + ENTRY_OVERHEAD;

/** An ID for a record, containing a page number and an entry ID in the page. */
export class RecordId {
    public readonly pageNum: PageNum;
    public readonly entryId: number;

    public constructor(pageNum: PageNum, entryId: number) {
        this.pageNum = pageNum;
        this.entryId = entryId;
    }

    public serialize(): string {
        return string.pack(RECORD_ID_FMT, this.pageNum, this.entryId);
    }

    public static deserialize(
        str: string,
        at?: number,
    ): LuaMultiReturn<[RecordId, number]> {
        const [pageNum, entryId, pos] = string.unpack(RECORD_ID_FMT, str, at);
        return $multi(new RecordId(pageNum, entryId), pos);
    }
}

export class RecordsComponent {
    /** The collection page size. */
    private pageSize: PageSize;

    /** A component for allocating pages. */
    private allocatedPages: AllocatedPageComponent;

    /** The namespace for the header page. */
    private headerNamespace: Namespace;

    /** The namespace for the record pages. */
    private pageNamespace: Namespace;

    /** How many entry bytes the record page can store. */
    private capacity: PageSize;

    /** The maximum supported record size that can be stored. */
    public readonly maxRecordSize: number;

    public constructor(
        collection: IStoreCollection<IPage, IPageStore<IPage>>,
        allocatedPages: AllocatedPageComponent,
        headerNamespace: Namespace,
        pageNamespace: Namespace,
    ) {
        assert(allocatedPages.pagesNamespace == pageNamespace);
        this.headerNamespace = headerNamespace;
        this.pageNamespace = pageNamespace;
        this.pageSize = collection.pageSize;
        this.capacity = RecordPageObj.getCapacity(this.pageSize);
        this.maxRecordSize = this.capacity - ENTRY_OVERHEAD;
        this.allocatedPages = allocatedPages;
    }

    public deserializeObj(namespace: Namespace, str?: string): IObj<IEvent> {
        if (namespace == this.pageNamespace) {
            return deserializeRecordPageObj(str);
        } else if (namespace == this.headerNamespace) {
            return deserializeHeaderObj(str);
        } else {
            throw new Error("Unknown namespace: " + namespace);
        }
    }

    public deserializeEv(namespace: Namespace, str: string): IEvent {
        if (namespace == this.pageNamespace) {
            return deserializeEntryEvent(str);
        } else if (namespace == this.headerNamespace) {
            return deserializeHeaderEvent(str);
        } else {
            throw new Error("Unknown namespace: " + namespace);
        }
    }

    public getRecord(
        collection: TxCollection,
        recordId: RecordId,
    ): string | undefined {
        return collection
            .getStoreCast<RecordPageObj, EntryEvent>(this.pageNamespace)
            .getPage(recordId.pageNum)
            .obj
            .entries
            .get(recordId.entryId);
    }

    private allocNewPage(collection: TxCollection, str: string): RecordId {
        const header = collection
            .getStoreCast<HeaderObj, HeaderEvent>(this.headerNamespace)
            .getPage(0 as PageNum);

        // Get a new page.
        const nextPage = this.allocatedPages
            .allocPageCast<RecordPageObj, EntryEvent>(collection);

        // Put the record in.
        const entryId = nextPage.obj.getUnusedEntryId();
        nextPage.doEvent(new CreateEntryEvent(entryId, str));

        // Get the page's size class.
        const usedSpace = nextPage.obj.usedSpace;
        const sizeClass = getSizeClass(this.capacity, usedSpace);

        // Push it into the start of the linked list.
        const headerLink = header.obj.links[sizeClass];
        nextPage.doEvent(new SetLinksEvent(sizeClass, NO_LINK, headerLink));
        header.doEvent(new HeaderEvent(sizeClass, nextPage.pageNum));

        return new RecordId(nextPage.pageNum, entryId);
    }

    /** Reassigns a page's size class if needed. */
    private reassignSizeClass(
        collection: TxCollection,
        page: TxPage<RecordPageObj, EntryEvent>,
    ) {
        const header = collection
            .getStoreCast<HeaderObj, HeaderEvent>(this.headerNamespace)
            .getPage(0 as PageNum);
        const pages = collection
            .getStoreCast<RecordPageObj, EntryEvent>(this.pageNamespace);

        const usedSpace = page.obj.usedSpace;
        const sizeClass = page.obj.sizeClass;
        const newSizeClass = getSizeClass(this.capacity, usedSpace, sizeClass);

        if (newSizeClass != sizeClass || usedSpace == 0) {
            // Unlink from the header or the previous page.
            if (page.obj.prev == NO_LINK) {
                header.doEvent(new HeaderEvent(sizeClass, page.obj.next));
            } else {
                const prevPage = pages.getPage(page.obj.prev);
                prevPage.doEvent(new SetLinksEvent(
                    prevPage.obj.sizeClass,
                    prevPage.obj.prev,
                    page.obj.next,
                ));
            }

            // Unlink from the next page.
            if (page.obj.next != NO_LINK) {
                const nextPage = pages.getPage(page.obj.next);
                nextPage.doEvent(new SetLinksEvent(
                    nextPage.obj.sizeClass,
                    page.obj.prev,
                    nextPage.obj.next,
                ));
            }
        }

        if (usedSpace == 0) {
            // Free the page.
            page.doEvent(new SetLinksEvent(0 as SizeClass, NO_LINK, NO_LINK));
            this.allocatedPages.freeUnusedPages(collection, page.pageNum);
        } else if (newSizeClass != sizeClass) {
            // Push it into the start of the new class list.
            const headerLink = header.obj.links[newSizeClass];
            page.doEvent(new SetLinksEvent(newSizeClass, NO_LINK, headerLink));
            header.doEvent(new HeaderEvent(newSizeClass, page.pageNum));
        }
    }

    private allocExistingPage(
        collection: TxCollection,
        pageNum: PageNum,
        str: string,
    ): RecordId {
        const page = collection
            .getStoreCast<RecordPageObj, EntryEvent>(this.pageNamespace)
            .getPage(pageNum);
        const entryId = page.obj.getUnusedEntryId();
        page.doEvent(new CreateEntryEvent(entryId, str));
        this.reassignSizeClass(collection, page);
        return new RecordId(pageNum, entryId);
    }

    /**
     * Allocates a new record, returning its ID.
     * @throws If the string length is larger than `maxRecordSize`.
     */
    public alloc(collection: TxCollection, str: string): RecordId {
        assert(str.length <= this.maxRecordSize);

        const header = collection
            .getStoreCast<HeaderObj, HeaderEvent>(this.headerNamespace)
            .getPage(0 as PageNum);

        // Do a best-fit search on a size class that fits our payload.
        const size = str.length + ENTRY_OVERHEAD;
        let sizeClass = getClassThatFits(this.capacity, size);
        if (sizeClass) {
            while (sizeClass >= 0 && header.obj.links[sizeClass] == NO_LINK) {
                sizeClass--;
            }
        }

        if (sizeClass && sizeClass >= 0) {
            return this.allocExistingPage(
                collection,
                header.obj.links[sizeClass],
                str,
            );
        } else {
            return this.allocNewPage(collection, str);
        }
    }

    /** Frees a previously allocated record. */
    public free(collection: TxCollection, recordId: RecordId) {
        const page = collection
            .getStoreCast<RecordPageObj, EntryEvent>(this.pageNamespace)
            .getPage(recordId.pageNum);
        page.doEvent(new DeleteEntryEvent(recordId.entryId));
        this.reassignSizeClass(collection, page);
    }

    /**
     * Reallocates a record, returning either the same or a new ID.
     * @throws If the string length is larger than `maxRecordSize`.
     */
    public realloc(
        collection: TxCollection,
        recordId: RecordId,
        str: string,
    ): RecordId {
        assert(str.length <= this.maxRecordSize);
        const page = collection
            .getStoreCast<RecordPageObj, EntryEvent>(this.pageNamespace)
            .getPage(recordId.pageNum);
        const oldEntry = assert(page.obj.entries.get(recordId.entryId));
        const delta = str.length - oldEntry.length;
        const freeSpace = this.capacity - page.obj.usedSpace;
        if (delta <= freeSpace) {
            // Reallocate in-place.
            page.doEvent(new WriteEntryEvent(recordId.entryId, str));
            this.reassignSizeClass(collection, page);
            return recordId;
        } else {
            // Reallocate out-of-place.
            this.free(collection, recordId);
            return this.alloc(collection, str);
        }
    }
}
