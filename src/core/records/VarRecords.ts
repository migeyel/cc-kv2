import { Namespace } from "../store/IPageStore";
import { IEvent, IObj, TxCollection } from "../txStore/LogStore";
import { RECORD_ID_BYTES, RecordId, RecordsComponent } from "./Records";

const VID_LENGTH_BYTES = 2;
const VID_LEN_FMT = "<I" + VID_LENGTH_BYTES;
const MAX_VID_PREFIX_LENGTH = 256 ** VID_LENGTH_BYTES / 2 - 1;

/**
 * An id referring to a var record.
 *
 * To avoid indirections, the VID can contain a prefix to the record directly in
 * its body. The length of the prefix is defined by the user code, but can't be
 * longer than MAX_VID_PREFIX_LENGTH bytes.
 */
export class VarRecordId {
    /** The direct prefix. */
    public readonly str: string;

    /** A reference the rest of the record. */
    public readonly rid?: RecordId;

    public constructor(str: string, rid?: RecordId) {
        assert(str.length <= MAX_VID_PREFIX_LENGTH);
        this.str = str;
        this.rid = rid;
    }

    public serialize(): string {
        if (this.rid) {
            return string.pack(VID_LEN_FMT, 2 * this.str.length + 1) +
                this.str +
                this.rid.serialize();
        } else {
            return string.pack(VID_LEN_FMT, 2 * this.str.length) + this.str;
        }
    }

    public static deserialize(
        str: string,
        pos = 1,
    ): LuaMultiReturn<[VarRecordId, number]> {
        const [lenf] = string.unpack(VID_LEN_FMT, str, pos);
        const flag = lenf % 2;
        const len = (lenf - flag) / 2;
        const dstrStart = pos + VID_LENGTH_BYTES;
        const dstrEnd = dstrStart + len - 1;
        const dstr = string.sub(str, dstrStart, dstrEnd);
        const pos2 = dstrEnd + 1;
        if (flag == 0) {
            return $multi(new VarRecordId(dstr), pos2);
        } else {
            const [rid, pos3] = RecordId.deserialize(str, pos2);
            return $multi(new VarRecordId(dstr, rid), pos3);
        }
    }

    public length(): number {
        if (this.rid) {
            return VID_LENGTH_BYTES + this.str.length + RECORD_ID_BYTES;
        } else {
            return VID_LENGTH_BYTES + this.str.length;
        }
    }
}

/**
 * Abstraction over a RecordsComponent for storing arbitrary size records.
 *
 * Records are split up into one or more bounded records, each holding a
 * reference to the next one, and are addressed by a variable record ID, which
 * may also contain a prefix of the record for better performance.
 */
export class VarRecordsComponent {
    /** The bounded-size records component that stores var record slices. */
    public records: RecordsComponent;

    /** The maximum byte size taken by a VID allocated on this component. */
    public readonly maxVidLen: number;

    public constructor(
        recordsComponent: RecordsComponent,
        maxVidLen: number,
    ) {
        // Must be able to fit the length bytes and the body record id.
        assert(maxVidLen >= VID_LENGTH_BYTES + RECORD_ID_BYTES);

        // Length must be bounded by the global max length.
        maxVidLen = math.min(maxVidLen, MAX_VID_PREFIX_LENGTH);

        this.records = recordsComponent;
        this.maxVidLen = maxVidLen;
    }

    public deserializeObj(n: Namespace, s?: string): IObj<IEvent> | undefined {
        return this.records.deserializeObj(n, s);
    }

    public deserializeEv(n: Namespace, s: string): IEvent | undefined {
        return this.records.deserializeEv(n, s);
    }

    /** Allocates a record. */
    public allocate(collection: TxCollection, record: string): VarRecordId {
        const maxDirectLen = this.maxVidLen - VID_LENGTH_BYTES;
        if (record.length <= maxDirectLen) {
            // Put everything directly into the VID.
            return new VarRecordId(record);
        }

        // Split the start to put into the VID.
        const maxIndirectStrLen = maxDirectLen - RECORD_ID_BYTES;
        const head = string.sub(record, 1, maxIndirectStrLen);
        record = string.sub(record, maxIndirectStrLen + 1);

        // Split the rest into one or more bounded records.
        const splits = [];
        const maxSplitLen = this.records.maxRecordSize - RECORD_ID_BYTES;
        while (record.length > 0) {
            splits.push(string.sub(record, 1, maxSplitLen));
            record = string.sub(record, 1 + maxSplitLen);
        }

        // Allocate and link sub-records.
        let rid = this.records.alloc(collection, splits.pop()!);
        while (splits.length > 0) {
            const str = splits.pop() + rid.serialize();
            rid = this.records.alloc(collection, str);
        }

        const out = new VarRecordId(head, rid);
        assert(out.length() <= this.maxVidLen);
        return out;
    }

    /** Deallocates a record. */
    public free(collection: TxCollection, vid: VarRecordId): void {
        let rid = vid.rid;
        while (rid) {
            const rstr = assert(this.records.getRecord(collection, rid));
            this.records.free(collection, rid);
            if (rstr.length < this.records.maxRecordSize) {
                break;
            } else {
                rid = RecordId.deserialize(rstr, -RECORD_ID_BYTES)[0];
            }
        }
    }

    /** Iterates through the record's sub-record strings. */
    public iter(
        collection: TxCollection,
        vid: VarRecordId,
    ): LuaIterable<string> {
        let rid: RecordId | undefined;
        // @ts-expect-error: This works fine they just don't like it.
        return (_, prev?: string): string => {
            if (!prev) {
                rid = vid.rid;
                return vid.str || "";
            }

            if (rid) {
                const rstr = assert(this.records.getRecord(collection, rid));
                if (rstr.length < this.records.maxRecordSize) {
                    rid = undefined;
                    return rstr;
                } else {
                    rid = RecordId.deserialize(rstr, -RECORD_ID_BYTES)[0];
                    return string.sub(rstr, 1, -1 - RECORD_ID_BYTES);
                }
            }
        };
    }

    /**
     * Compares a string to a record.
     * @returns
     * - `-1` if the string is smaller than the record.
     * - `0` if the string is equal to the record.
     * - `1` if the string is larger than the record.
     */
    public cmp(
        collection: TxCollection,
        str: string,
        vid: VarRecordId,
    ): -1 | 0 | 1 {
        for (const slice2 of this.iter(collection, vid)) {
            const slice1 = string.sub(str, 1, slice2.length);
            str = string.sub(str, slice2.length + 1);
            if (slice1 != slice2) {
                if (slice1 < slice2) {
                    return -1;
                } else {
                    return 1;
                }
            }
        }

        if (str.length > 0) {
            return 1;
        } else {
            return 0;
        }
    }

    /** Reads the record and returns it as a string. */
    public read(collection: TxCollection, vid: VarRecordId): string {
        const out = [];
        for (const s of this.iter(collection, vid)) { out.push(s); }
        return table.concat(out);
    }
}
