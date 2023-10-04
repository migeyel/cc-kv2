import { TxId } from "../LogStore";
import { RecordType, TX_ID_FMT } from "./types";

const FMT = "<B" + TX_ID_FMT;

export type Record = {
    ty: RecordType.COMMIT,

    /** Which transaction committed. */
    txId: TxId,
}

export function serialize(r: Record): string {
    return string.pack(FMT, r.ty, r.txId);
}

export function deserialize(str: string): Record {
    const [_, txId] = string.unpack(FMT, str);
    return { ty: RecordType.COMMIT, txId };
}
