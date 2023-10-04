import { uIntLenBytes } from "../../util";
import { MAX_EVENT_RECORD_LEN } from "./EventSubRecord";
import { LSN_FMT, RecordType, TX_ID_FMT } from "./types";
import * as EventSubRecord from "./EventSubRecord";
import { TxId } from "../LogStore";

/** The maximum length a serialized act can have. */
export const MAX_UNDO_INFO_LEN = 2 ** 32 - 1;

const EVENT_FMT = "<s" + uIntLenBytes(MAX_EVENT_RECORD_LEN);
const UNDO_INFO_FMT = "<s" + uIntLenBytes(MAX_UNDO_INFO_LEN);
const FMT = "<B" + TX_ID_FMT + LSN_FMT + UNDO_INFO_FMT;

export type Record = {
    ty: RecordType.ACT,

    /** In which transaction the act happened. */
    txId: TxId,

    /** The LSN of the previous act that happened in the same transaction. */
    prevLsn: number,

    /** The serialized undo info for the act. */
    undoInfo: string,

    /** The events that happened in this act. */
    events: EventSubRecord.Record[],
};

export function serialize(r: Record): string {
    const out = [string.pack(FMT, r.ty, r.txId, r.prevLsn, r.undoInfo)];
    for (const event of r.events) {
        out.push(string.pack(EVENT_FMT, EventSubRecord.serialize(event)));
    }
    return table.concat(out);
}

export function deserialize(str: string): Record {
    const [
        _,
        txId,
        prevLsn,
        undoInfo,
        pos,
    ] = string.unpack(FMT, str);

    let cur = pos;
    const events = [];
    while (cur <= str.length) {
        const [eventStr, nextCur] = string.unpack(EVENT_FMT, str, cur);
        events.push(EventSubRecord.deserialize(eventStr));
        cur = nextCur;
    }

    return { ty: RecordType.ACT, txId, prevLsn, undoInfo, events };
}
