import { NO_LINK } from "../records/RecordPage";
import { RECORD_ID_BYTES } from "../records/Records";
import { VarRecordId, VarRecordsComponent } from "../records/VarRecords";
import { PageNum, PAGE_LINK_BYTES } from "../store/IPageStore";
import { PAGE_FMT } from "../txStore/LogRecord/types";
import { IEvent, IObj } from "../txStore/LogStore";
import { uIntLenBytes } from "../util";

/** The maximum amount of entries that can fit in a leaf node. */
export const MAX_LEAF_ENTRY = 65534;

/** How many bytes it takes to store an index into a leaf entry. */
const ENTRY_IDX_BYTES = uIntLenBytes(MAX_LEAF_ENTRY);
const ENTRY_LEN_BYTES = uIntLenBytes(MAX_LEAF_ENTRY + 1);
const ENTRY_IDX_FMT = "I" + ENTRY_IDX_BYTES;
const ENTRY_LEN_FMT = "I" + ENTRY_LEN_BYTES;

/** Pessimistic lower limit on page size for leaves. */
export const MAX_LEAF_PAGE_SIZE = MAX_LEAF_ENTRY * RECORD_ID_BYTES;

enum LeafEventType {
    ADD_ENTRY,
    DEL_ENTRY,
    SET_ENTRY,
    SET_LINKS,
}

const ADD_LEAF_FMT = "<B" + ENTRY_IDX_FMT;

export class AddLeafEntryEvent implements IEvent {
    public ty = LeafEventType.ADD_ENTRY as const;
    public pos: number;
    public value: VarRecordId;
    public key: VarRecordId;

    public constructor(pos: number, value: VarRecordId, key: VarRecordId) {
        this.pos = pos;
        this.value = value;
        this.key = key;
    }

    public serialize(): string {
        return string.pack(ADD_LEAF_FMT, this.ty, this.pos) +
            this.value.serialize() +
            this.key.serialize();
    }

    public static deserialize(str: string): AddLeafEntryEvent {
        const [_, pos, a1] = string.unpack(ADD_LEAF_FMT, str);
        const [value, a2] = VarRecordId.deserialize(str, a1);
        const [key] = VarRecordId.deserialize(str, a2);
        return new AddLeafEntryEvent(pos, value, key);
    }
}

const DEL_LEAF_FMT = "<B" + ENTRY_IDX_FMT;

export class DelLeafEntryEvent implements IEvent {
    public ty = LeafEventType.DEL_ENTRY as const;
    public pos: number;

    public constructor(pos: number) {
        this.pos = pos;
    }

    public serialize(): string {
        return string.pack(DEL_LEAF_FMT, this.ty, this.pos);
    }

    public static deserialize(str: string): DelLeafEntryEvent {
        const [_, pos] = string.unpack(DEL_LEAF_FMT, str);
        return new DelLeafEntryEvent(pos);
    }
}

const SET_LEAF_FMT = "<B" + ENTRY_IDX_FMT;

export class SetLeafEntryEvent implements IEvent {
    public ty = LeafEventType.SET_ENTRY as const;
    public pos: number;
    public value: VarRecordId;

    public constructor(pos: number, value: VarRecordId) {
        this.pos = pos;
        this.value = value;
    }

    public serialize(): string {
        return string.pack(SET_LEAF_FMT, this.ty, this.pos) +
            this.value.serialize();
    }

    public static deserialize(str: string): SetLeafEntryEvent {
        const [_, pos, a1] = string.unpack(SET_LEAF_FMT, str);
        const [value] = VarRecordId.deserialize(str, a1);
        return new SetLeafEntryEvent(pos, value);
    }
}

const SET_LINKS_FMT = "<B" + PAGE_FMT + PAGE_FMT;

export class SetLeafLinksEvent implements IEvent {
    public ty = LeafEventType.SET_LINKS as const;
    public prev?: PageNum;
    public next?: PageNum;

    public constructor(prev?: PageNum, next?: PageNum) {
        this.prev = prev;
        this.next = next;
    }

    public serialize(): string {
        return string.pack(
            SET_LINKS_FMT,
            this.ty,
            this.prev || NO_LINK,
            this.next || NO_LINK,
        );
    }

    public static deserialize(str: string): SetLeafLinksEvent {
        const [_, prev, next] = string.unpack(SET_LINKS_FMT, str);
        return new SetLeafLinksEvent(
            prev == NO_LINK ? undefined : prev,
            next == NO_LINK ? undefined : next,
        );
    }
}

export type LeafEvent =
    | AddLeafEntryEvent
    | DelLeafEntryEvent
    | SetLeafEntryEvent
    | SetLeafLinksEvent;

const LEAF_FMT = "<" + ENTRY_LEN_FMT + PAGE_FMT + PAGE_FMT;

/** The byte overhead for a leaf page with no entries. */
export const LEAF_OVERHEAD = 1 + ENTRY_IDX_BYTES + 2 * PAGE_LINK_BYTES;

/** A B+ tree leaf node with sorted keys. */
export class LeafObj implements IObj<LeafEvent> {
    public readonly type = "leaf";

    /** Leaves always have 0 height. */
    public readonly height = 0;

    /** Key records for stored entries. */
    public keys: VarRecordId[];

    /** Value records for stored entries, one for each key. */
    public vals: VarRecordId[];

    /** The previous node in the leaf linked list, if any. */
    public prev?: PageNum;

    /** The next node in the leaf linked list, if any. */
    public next?: PageNum;

    /**
     * The total bytes taken by all entries in this node.
     *
     * Taken together with `LEAF_OVERHEAD` represents the total amount of bytes
     * the serialized object takes when nonempty.
     */
    public usedSpace: number;

    public constructor(
        vals: VarRecordId[],
        keys: VarRecordId[],
        prev?: PageNum,
        next?: PageNum,
    ) {
        this.vals = vals;
        this.keys = keys;
        this.prev = prev;
        this.next = next;
        this.usedSpace = 0;
        for (const v of vals) { this.usedSpace += v.length(); }
        for (const k of keys) { this.usedSpace += k.length(); }
    }

    /** Returns the maximum byte size a leaf entry can take in the node. */
    public static getMaxEntrySize(vrc: VarRecordsComponent) {
        return 2 * vrc.maxVidLen;
    }

    /** Returns a reasonable index to split this node in half. */
    public getSplitIndex(): number {
        let sum = 0;
        let index = 0;
        while (sum < this.usedSpace / 2) {
            sum += this.keys[index].length();
            sum += this.vals[index].length();
            index++;
        }
        return index;
    }

    public apply(event: LeafEvent): void {
        if (event.ty == LeafEventType.ADD_ENTRY) {
            table.insert(this.vals, event.pos + 1, event.value);
            table.insert(this.keys, event.pos + 1, event.key);
            this.usedSpace += event.value.length() + event.key.length();
        } else if (event.ty == LeafEventType.SET_LINKS) {
            this.prev = event.prev;
            this.next = event.next;
        } else if (event.ty == LeafEventType.SET_ENTRY) {
            assert(this.keys[event.pos], "can't set a non-existant entry");
            this.vals[event.pos] = event.value;
        } else if (event.ty == LeafEventType.DEL_ENTRY) {
            assert(this.keys[event.pos], "can't delete a non-existant entry");
            this.usedSpace -= table.remove(this.vals, event.pos + 1)!.length();
            this.usedSpace -= table.remove(this.keys, event.pos + 1)!.length();
        } else {
            event satisfies never;
        }
    }

    public isEmpty(): boolean {
        return this.vals.length == 0 &&
            this.keys.length == 0 &&
            this.prev == undefined &&
            this.next == undefined;
    }

    public serialize(): string {
        const out = [string.pack(
            LEAF_FMT,
            this.keys.length,
            this.prev || NO_LINK,
            this.next || NO_LINK,
        )];
        for (const v of this.vals) { out.push(v.serialize()); }
        for (const k of this.keys) { out.push(k.serialize()); }
        return table.concat(out);
    }
}

export function deserializeLeafObj(str?: string): LeafObj {
    if (!str) { return new LeafObj([], []); }

    const [keysLength, prev, next, a1] = string.unpack(LEAF_FMT, str);
    let pos = a1;

    const vals = <VarRecordId[]>[];
    for (const _ of $range(1, keysLength)) {
        const [val, nextPos] = VarRecordId.deserialize(str, pos);
        vals.push(val);
        pos = nextPos;
    }

    const keys = <VarRecordId[]>[];
    for (const _ of $range(1, keysLength)) {
        const [key, nextPos] = VarRecordId.deserialize(str, pos);
        keys.push(key);
        pos = nextPos;
    }

    return new LeafObj(
        vals,
        keys,
        prev == NO_LINK ? undefined : prev,
        next == NO_LINK ? undefined : next,
    );
}

export function deserializeLeafEvent(str: string): LeafEvent {
    const ty = string.byte(str);
    if (ty == LeafEventType.ADD_ENTRY) {
        return AddLeafEntryEvent.deserialize(str);
    } else if (ty == LeafEventType.DEL_ENTRY) {
        return DelLeafEntryEvent.deserialize(str);
    } else if (ty == LeafEventType.SET_ENTRY) {
        return SetLeafEntryEvent.deserialize(str);
    } else if (ty == LeafEventType.SET_LINKS) {
        return SetLeafLinksEvent.deserialize(str);
    } else {
        throw new Error("Unknown leaf event type: " + ty);
    }
}
