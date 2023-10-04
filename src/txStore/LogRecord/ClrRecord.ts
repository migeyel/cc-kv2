import { uIntLenBytes } from "../../util";
import { MAX_EVENT_RECORD_LEN } from "./EventSubRecord";
import { LSN_FMT, RecordType, TX_ID_FMT } from "./types";
import * as EventSubRecord from "./EventSubRecord";
import { TxId } from "../LogStore";

const EVENT_FMT = "<s" + uIntLenBytes(MAX_EVENT_RECORD_LEN);
const CLR_FMT = "<B" + TX_ID_FMT + LSN_FMT + LSN_FMT;

export type Record = {
    ty: RecordType.CLR,

    /** In which transaction the act happened. */
    txId: TxId,

    /** The LSN of the previous act that happened in the same transaction. */
    prevLsn: number,

    /** The LSN of the next act to undo in the undo chain. */
    undoNxtLsn: number,

    /** The events that happened in this act. */
    events: EventSubRecord.Record[],
};

export function serialize(r: Record): string {
    const out = [string.pack(
        CLR_FMT,
        r.ty,
        r.txId,
        r.prevLsn,
        r.undoNxtLsn,
    )];

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
        undoNxtLsn,
        pos,
    ] = string.unpack(CLR_FMT, str);

    let cur = pos;
    const events = [];
    while (cur <= str.length) {
        const [eventStr, nextCur] = string.unpack(EVENT_FMT, str, cur);
        events.push(EventSubRecord.deserialize(eventStr));
        cur = nextCur;
    }

    return { ty: RecordType.CLR, txId, prevLsn, undoNxtLsn, events };
}
