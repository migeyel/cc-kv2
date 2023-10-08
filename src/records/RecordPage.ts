import { PageNum, PageSize } from "../store/IPageStore";
import { IEvent, IObj } from "../txStore/LogStore";
import { PAGE_LINK_BYTES, SIZE_CLASS_BYTES, SizeClass } from "./SizeClass";

enum EventType {
    CREATE_ENTRY,
    DELETE_ENTRY,
    WRITE_ENTRY,
    SET_LINKS,
}

export const ENTRY_ID_BYTES = 2;

const ENTRY_LEN_BYTES = 2;

/** The page data strucutre overhead in bytes. */
const PAGE_OVERHEAD = SIZE_CLASS_BYTES + 2 * PAGE_LINK_BYTES;

/** The byte overhead of storing an entry in the page. */
export const ENTRY_OVERHEAD = ENTRY_ID_BYTES + ENTRY_LEN_BYTES;

/** The maximum entry ID that will be stored in the page. */
export const MAX_ENTRY_ID = 256 ** ENTRY_ID_BYTES - 1;

/** The maximum page size supported. */
const MAX_PAGE_SIZE = math.min(
    MAX_ENTRY_ID - PAGE_OVERHEAD,
    256 ** ENTRY_LEN_BYTES - 1,
);

/** A sentinel value for the linked list, meaning end of list. */
export const NO_LINK = 256 ** PAGE_LINK_BYTES - 1 as PageNum;

/** The packstring for a page's heading data structures. */
const PAGE_FMT = "<I" + SIZE_CLASS_BYTES +
    "I" + PAGE_LINK_BYTES +
    "I" + PAGE_LINK_BYTES;

/** The packstring for a page entry. */
const ENTRY_FMT = "<I" + ENTRY_ID_BYTES + "s" + ENTRY_LEN_BYTES;

/** A page object that holds user string records and bookkeeping information. */
export class RecordPageObj implements IObj<EntryEvent> {
    /** Tracks how many bytes all entries take, including overheads. */
    public usedSpace: number;

    /** Which size class the page is currently in. */
    public sizeClass: SizeClass;

    /** A pointer to the previous page on the size class linked list. */
    public prev: PageNum;

    /** A pointer to the next page on the size class linked list. */
    public next: PageNum;

    /** The stored entry strings, each indexed by an entry ID number. */
    public entries: LuaMap<number, string>;

    public constructor(
        entries: LuaMap<number, string>,
        sizeClass: SizeClass,
        prev: PageNum,
        next: PageNum,
    ) {
        this.entries = entries;
        this.sizeClass = sizeClass;
        this.prev = prev;
        this.next = next;
        this.usedSpace = 0;
        for (const [_, value] of this.entries) {
            this.usedSpace += ENTRY_ID_BYTES + ENTRY_LEN_BYTES;
            this.usedSpace += value.length;
        }
    }

    public static getCapacity(pageSize: PageSize): PageSize {
        assert(pageSize <= MAX_PAGE_SIZE);
        return pageSize - PAGE_OVERHEAD as PageSize;
    }

    public getUnusedEntryId(): number {
        let out = 0;
        while (this.entries.has(out)) { out++; }
        return out;
    }

    public apply(event: EntryEvent): void {
        if (event.ty == EventType.CREATE_ENTRY) {
            this.entries.set(event.entryId, event.value);
            this.usedSpace += ENTRY_ID_BYTES + ENTRY_LEN_BYTES;
            this.usedSpace += event.value.length;
        } else if (event.ty == EventType.DELETE_ENTRY) {
            this.usedSpace -= ENTRY_ID_BYTES + ENTRY_LEN_BYTES;
            this.usedSpace -= assert(this.entries.get(event.entryId)).length;
            this.entries.delete(event.entryId);
        } else if (event.ty == EventType.WRITE_ENTRY) {
            this.usedSpace -= assert(this.entries.get(event.entryId)).length;
            this.entries.set(event.entryId, event.value);
            this.usedSpace += event.value.length;
        } else if (event.ty == EventType.SET_LINKS) {
            this.sizeClass = event.sizeClass;
            this.prev = event.prev;
            this.next = event.next;
        }
    }

    public isEmpty(): boolean {
        return next(this.entries)[0] == undefined &&
            this.prev == NO_LINK &&
            this.next == NO_LINK &&
            this.sizeClass == 0;
    }

    public serialize(): string {
        const t = [string.pack(PAGE_FMT, this.sizeClass, this.prev, this.next)];
        for (const [id, value] of this.entries) {
            t.push(string.pack(ENTRY_FMT, id, value));
        }
        return table.concat(t);
    }
}

const CREATE_ENTRY_FMT = "<B" + "I" + ENTRY_ID_BYTES;

/** Event for creating a new entry. */
export class CreateEntryEvent implements IEvent {
    public ty = EventType.CREATE_ENTRY as const;
    public entryId: number;
    public value: string;

    public constructor(entryId: number, value: string) {
        this.entryId = entryId;
        this.value = value;
    }

    public serialize(): string {
        return string.pack(
            CREATE_ENTRY_FMT,
            this.ty,
            this.entryId,
        ) + this.value;
    }

    public static deserialize(str: string): CreateEntryEvent {
        const [_, entryid, pos] = string.unpack(CREATE_ENTRY_FMT, str);
        return new CreateEntryEvent(entryid, string.sub(str, pos));
    }
}

const DELETE_ENTRY_FMT = "<B" + "I" + ENTRY_ID_BYTES;

/** Event for deleting an entry. */
export class DeleteEntryEvent implements IEvent {
    public ty = EventType.DELETE_ENTRY as const;
    public entryId: number;

    public constructor(entryId: number) {
        this.entryId = entryId;
    }

    public serialize(): string {
        return string.pack(
            DELETE_ENTRY_FMT,
            this.ty,
            this.entryId,
        );
    }

    public static deserialize(str: string): DeleteEntryEvent {
        const [_, entryId] = string.unpack(DELETE_ENTRY_FMT, str);
        return new DeleteEntryEvent(entryId);
    }
}

const WRITE_ENTRY_FMT = "<B" + "I" + ENTRY_ID_BYTES + "s" + ENTRY_LEN_BYTES;

/** Event for overwriting an entry in-place. */
export class WriteEntryEvent implements IEvent {
    public ty = EventType.WRITE_ENTRY as const;
    public entryId: number;
    public value: string;

    public constructor(entryId: number, value: string) {
        this.entryId = entryId;
        this.value = value;
    }

    public serialize(): string {
        return string.pack(
            WRITE_ENTRY_FMT,
            this.ty,
            this.entryId,
            this.value,
        );
    }

    public static deserialize(str: string): WriteEntryEvent {
        const [_, entryId, value] = string.unpack(WRITE_ENTRY_FMT, str);
        return new WriteEntryEvent(entryId, value);
    }
}

const SET_LINK_FMT = "<B" +
    "I" + SIZE_CLASS_BYTES +
    "I" + PAGE_LINK_BYTES +
    "I" + PAGE_LINK_BYTES;

/** Event for setting the page's linked-list links and size class. */
export class SetLinksEvent implements IEvent {
    public ty = EventType.SET_LINKS as const;
    public sizeClass: SizeClass;
    public prev: PageNum;
    public next: PageNum;

    public constructor(
        sizeClass: SizeClass,
        prev: PageNum,
        next: PageNum,
    ) {
        this.sizeClass = sizeClass;
        this.prev = prev;
        this.next = next;
    }

    public serialize(): string {
        return string.pack(
            SET_LINK_FMT,
            this.ty,
            this.sizeClass,
            this.prev,
            this.next,
        );
    }

    public static deserialize(str: string): SetLinksEvent {
        const [
            _,
            sizeClass,
            prev,
            next,
        ] = string.unpack(SET_LINK_FMT, str);
        return new SetLinksEvent(sizeClass, prev, next);
    }
}

export type EntryEvent =
    | CreateEntryEvent
    | DeleteEntryEvent
    | WriteEntryEvent
    | SetLinksEvent;

export function deserializeRecordPageObj(str?: string): RecordPageObj {
    if (str) {
        const entries = new LuaMap<number, string>();
        const [sizeClass, prev, next, pos] = string.unpack(PAGE_FMT, str);
        let at = pos;
        while (at <= str.length) {
            const [id, value, nextAt] = string.unpack(ENTRY_FMT, str, at);
            entries.set(id, value);
            at = nextAt;
        }
        return new RecordPageObj(entries, sizeClass, prev, next);
    } else {
        return new RecordPageObj(
            new LuaMap(),
            0 as SizeClass,
            NO_LINK,
            NO_LINK,
        );
    }
}

export function deserializeEntryEvent(str: string): EntryEvent {
    const ty = string.byte(str, 1);
    if (ty == EventType.CREATE_ENTRY) {
        return CreateEntryEvent.deserialize(str);
    } else if (ty == EventType.DELETE_ENTRY) {
        return DeleteEntryEvent.deserialize(str);
    } else if (ty == EventType.WRITE_ENTRY) {
        return WriteEntryEvent.deserialize(str);
    } else if (ty == EventType.SET_LINKS) {
        return SetLinksEvent.deserialize(str);
    } else {
        throw new Error("Unknown event type: " + ty);
    }
}
