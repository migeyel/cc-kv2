import { LockedResource } from "../lock/Lock";
import { RecordLog } from "../RecordLog";
import { Namespace, PageNum } from "../store/IPageStore";
import { AnyTxPage, TxId } from "./LogStore";
import { DptEntry, TtEntry } from "./LogRecord/CheckpointRecord";
import * as EventSubRecord from "./LogRecord/EventSubRecord";

/** The state kept by a collection while an act is being processed. */
export class ActState {
    /** The set of pages pinned by the act. */
    public pinnedPages = new LuaSet<AnyTxPage>();

    /** The set of events applied in the act so far */
    public events: EventSubRecord.Record[] = [];

    public pushEvent(event: EventSubRecord.Record) {
        this.events.push(event);
    }
}

export class State {
    /** State for the current act taking place in the collection. */
    public actState?: ActState;

    /** Synchronization to ensure only one act can happen at any moment. */
    public actRes = new LockedResource();

    /** Transaction table. */
    public tt: LuaMap<TxId, TtEntry>;

    /** Dirty page table. */
    public dpt: LuaMap<Namespace, LuaMap<PageNum, DptEntry>>;

    /** The log structure for logging changes to the collection. */
    public log: RecordLog;

    /** Queries the TT for a transaction, creating a new one if none exists. */
    public getTtEntry(id: TxId): TtEntry {
        const out = this.tt.get(id);
        if (out) { return out; }
        const newOut = <TtEntry>{
            id,
            firstLsn: 0,
            lastLsn: 0,
            undoNxtLsn: 0,
        };
        this.tt.set(id, newOut);
        return newOut;
    }

    /** Deletes a transaction from the TT. */
    public deleteTtEntry(id: TxId) {
        this.tt.delete(id);
    }

    /** Queries the DPT for a dirty page. */
    public getDptEntry(
        namespace: Namespace,
        pageNum: PageNum,
    ): DptEntry | undefined {
        const nsTable = this.dpt.get(namespace);
        if (!nsTable) { return; }
        return nsTable.get(pageNum);
    }

    /** Modifies a page in the DPT. */
    public setDptEntry(
        namespace: Namespace,
        pageNum: PageNum,
        recLsn: number,
    ): void {
        let nsTable = this.dpt.get(namespace);
        if (!nsTable) {
            nsTable = new LuaMap();
            this.dpt.set(namespace, nsTable);
        }
        let out = nsTable.get(pageNum);
        if (!out) {
            out = { namespace, pageNum, recLsn };
            nsTable.set(pageNum, out);
        }
    }

    /** Removes a page from the DPT. */
    public deleteDptEntry(namespace: Namespace, pageNum: PageNum): void {
        const nsTable = this.dpt.get(namespace);
        if (!nsTable) { return; }
        nsTable.delete(pageNum);
        if (next(nsTable)[0] == undefined) { this.dpt.delete(namespace); }
    }

    public constructor(
        tt: LuaMap<TxId, TtEntry>,
        dpt: LuaMap<Namespace, LuaMap<PageNum, DptEntry>>,
        log: RecordLog,
    ) {
        this.tt = tt;
        this.dpt = dpt;
        this.log = log;
    }
}
