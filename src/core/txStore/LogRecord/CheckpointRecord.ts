import { Namespace, PageNum } from "../../store/IPageStore";
import { TxId } from "../LogStore";
import {
    DPT_LEN_FMT,
    LSN_FMT,
    NAMESPACE_FMT,
    PAGE_FMT,
    RecordType,
    TX_ID_FMT,
} from "./types";

/** A transaction table entry. */
export type TtEntry = {
    id: TxId,
    firstLsn: number,
    lastLsn: number,
    undoNxtLsn: number,
};

/** A dirty page table entry. */
export type DptEntry = {
    namespace: Namespace,
    pageNum: PageNum,
    recLsn: number,
}

const TT_FMT = "<" + TX_ID_FMT + LSN_FMT + LSN_FMT + LSN_FMT;
const DPT_FMT = "<" + NAMESPACE_FMT + PAGE_FMT + LSN_FMT;
const FMT = "<B" + TX_ID_FMT + DPT_LEN_FMT;

export type Record = {
    ty: RecordType.CHECKPOINT,

    /** The checkpoint transaction table. */
    tt: TtEntry[],

    /** The checkpoint dirty page table. */
    dpt: DptEntry[],
}

export function serialize(r: Record) {
    const out = [string.pack(
        FMT,
        r.ty,
        r.tt.length,
        r.dpt.length,
    )];

    for (const t of r.tt) {
        out.push(string.pack(
            TT_FMT,
            t.id,
            t.firstLsn,
            t.lastLsn,
            t.undoNxtLsn,
        ));
    }

    for (const p of r.dpt) {
        out.push(string.pack(DPT_FMT, p.namespace, p.pageNum, p.recLsn));
    }

    return table.concat(out);
}

export function deserialize(str: string): Record {
    const [_, ttLen, dptLen, pos] = string.unpack(FMT, str);
    let cur = pos;

    const tt = <TtEntry[]>[];
    for (const i of $range(1, ttLen)) {
        const [
            id,
            firstLsn,
            lastLsn,
            undoNxtLsn,
            nextCur,
        ] = string.unpack(TT_FMT, str, cur);
        tt[i - 1] = { id, firstLsn, lastLsn, undoNxtLsn };
        cur = nextCur;
    }

    const dpt = <DptEntry[]>[];
    for (const i of $range(1, dptLen)) {
        const [
            namespace,
            pageNum,
            recLsn,
            nextCur,
        ] = string.unpack(DPT_FMT, str, cur);
        dpt[i - 1] = { namespace, pageNum, recLsn };
        cur = nextCur;
    }

    return { ty: RecordType.CHECKPOINT, tt, dpt };
}
