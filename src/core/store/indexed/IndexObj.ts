
import { IEvent, IObj } from "../../txStore/LogStore";
import { uIntLenBytes } from "../../util";

/** The maximum number of entries in a single index page. */
export const MAX_INDEX_PAGE_ENTRIES = 65535;

/** The maximum number of substores the index can refer to. */
export const MAX_INDEXED_SUBSTORES = 65534;

/** How many bytes it takes to identify a substore. */
export const SUBSTORE_BYTELEN = uIntLenBytes(MAX_INDEXED_SUBSTORES + 1);

export const SUBSTORE_LFMT = "I" + SUBSTORE_BYTELEN;
export const SUBSTORE_FMT = "<I" + SUBSTORE_BYTELEN;
export const INDEX_POS_FMT = "<I" + uIntLenBytes(MAX_INDEX_PAGE_ENTRIES);

/** An object containing an index page. Used in mapping a page number to a substore. */
export class IndexObj implements IObj<SetIndexEntryEvent> {
    /** Maps a position to a reference to a substore (both numbers). */
    public substores: LuaMap<number, number>;

    public constructor(substores: LuaMap<number, number>) {
        this.substores = substores;
    }

    public apply(event: SetIndexEntryEvent): void {
        if (event.value == 0) {
            this.substores.delete(event.pos);
        } else {
            this.substores.set(event.pos, event.value);
        }
    }

    public isEmpty(): boolean {
        return this.substores.isEmpty();
    }

    public serialize(): string {
        let maxK = 0;
        for (const [k] of this.substores) { maxK = math.max(maxK, k); }
        const arr = <number[]>[];
        for (const i of $range(0, maxK)) { arr[i] = this.substores.get(i) || 0; }
        const fmt = "<" + string.rep(SUBSTORE_FMT, arr.length);
        return string.pack(fmt, table.unpack(arr));
    }

    public static deserialize(str: string): IndexObj {
        const size = str.length / SUBSTORE_BYTELEN;
        const fmt = "<" + string.rep(SUBSTORE_FMT, size);
        const arr = string.unpack(fmt, str);
        const substores = new LuaMap<number, number>();
        for (const i of $range(0, arr.length - 1)) {
            if (arr[i] != 0) {
                substores.set(i, arr[i]);
            }
        }
        return new IndexObj(substores);
    }
}

/** An event for setting or deleting an entry in an index page. */
export class SetIndexEntryEvent implements IEvent {
    public pos: number;
    public value: number;

    public constructor(pos: number, value: number) {
        this.pos = pos;
        this.value = value;
    }

    public serialize(): string {
        return string.pack(INDEX_POS_FMT + SUBSTORE_FMT, this.pos, this.value);
    }

    public static deserialize(str: string): SetIndexEntryEvent {
        const [pos, value] = string.unpack(INDEX_POS_FMT + SUBSTORE_FMT, str);
        return new SetIndexEntryEvent(pos, value);
    }
}
