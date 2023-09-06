import { RecordLog } from "../../RecordLog";
import {
    MAX_NAMESPACE_LEN,
    MAX_PAGE_NUM,
    Namespace,
    PageNum,
} from "../IPageStore";
import { MAX_INDEXED_SUBSTORES } from "./Index";

function fmtLen(maximum: number): string {
    return tostring(math.ceil(math.log(maximum + 1, 256)));
}

const SUB_STORE_FMT = "I" + fmtLen(MAX_INDEXED_SUBSTORES);
const PAGE_FMT = "I" + fmtLen(MAX_PAGE_NUM);
const NAMESPACE_FMT = "s" + fmtLen(MAX_NAMESPACE_LEN);

export enum RecordType {
    CHECKPOINT,
    CREATE_PAGE,
    DELETE_PAGE,
    CREATE_SUB_STORE,
    DELETE_SUB_STORE,
    MOVE_PAGE,
}

export type SubStoreNum = number & { readonly __brand: unique symbol };

type CheckpointRecord = {
    ty: RecordType.CHECKPOINT,
    substores: LuaMap<SubStoreNum, number>,
};

const CHECKPOINT_FMT_PRE = "<B" + SUB_STORE_FMT;

type CreatePage = {
    ty: RecordType.CREATE_PAGE,
    namespace: Namespace,
    pageNum: PageNum,
    where: SubStoreNum,
}

const CREATE_PAGE_FMT = "<B" + NAMESPACE_FMT + PAGE_FMT + SUB_STORE_FMT;

type DeletePage = {
    ty: RecordType.DELETE_PAGE,
    namespace: Namespace,
    pageNum: PageNum,
    where: SubStoreNum,
}

const DELETE_PAGE_FMT = "<B" + NAMESPACE_FMT + PAGE_FMT + SUB_STORE_FMT;

type CreateSubStore = {
    ty: RecordType.CREATE_SUB_STORE,
    where: SubStoreNum,
};

const CREATE_SUB_STORE_FMT = "<B" + SUB_STORE_FMT;

type DeleteSubStore = {
    ty: RecordType.DELETE_SUB_STORE,
    where: SubStoreNum,
};

const DELETE_SUB_STORE_FMT = "<B" + SUB_STORE_FMT;

type MovePage = {
    ty: RecordType.MOVE_PAGE,
    namespace: Namespace,
    pageNum: PageNum,
    from: SubStoreNum,
    to: SubStoreNum,
};

const MOVE_PAGE_FMT =
    "<B" + NAMESPACE_FMT + PAGE_FMT + SUB_STORE_FMT + SUB_STORE_FMT;

type ProcedureRecord =
    | CreatePage
    | DeletePage
    | CreateSubStore
    | DeleteSubStore
    | MovePage

export type IndexRecord = ProcedureRecord | CheckpointRecord

export function encodeRecord(record: IndexRecord): string {
    if (record.ty == RecordType.CHECKPOINT) {
        const stores = <number[]>[];
        for (const [num, used] of record.substores) {
            stores.push(num);
            stores.push(used);
        }
        const fmt = CHECKPOINT_FMT_PRE +
            string.rep(SUB_STORE_FMT + PAGE_FMT, stores.length / 2);
        return string.pack(fmt, record.ty, stores.length / 2, ...stores);
    } else if (record.ty == RecordType.CREATE_PAGE) {
        return string.pack(
            CREATE_PAGE_FMT,
            record.ty,
            record.namespace,
            record.pageNum,
            record.where,
        );
    } else if (record.ty == RecordType.DELETE_PAGE) {
        return string.pack(
            DELETE_PAGE_FMT,
            record.ty,
            record.namespace,
            record.pageNum,
            record.where,
        );
    } else if (record.ty == RecordType.CREATE_SUB_STORE) {
        return string.pack(
            CREATE_SUB_STORE_FMT,
            record.ty,
            record.where,
        );
    } else if (record.ty == RecordType.DELETE_SUB_STORE) {
        return string.pack(
            DELETE_PAGE_FMT,
            record.ty,
            record.where,
        );
    } else {
        return string.pack(
            MOVE_PAGE_FMT,
            record.ty,
            record.namespace,
            record.pageNum,
            record.from,
            record.to,
        );
    }
}

export function decodeRecord(str: string): IndexRecord {
    const ty = string.byte(str, 1);
    if (ty == RecordType.CHECKPOINT) {
        const [_, len, pos] = string.unpack(CHECKPOINT_FMT_PRE, str);
        const fmt = "<" + string.rep(SUB_STORE_FMT + PAGE_FMT, len);
        const stores: number[] = string.unpack(fmt, str, pos);
        const substores = new LuaMap<SubStoreNum, number>();
        for (const i of $range(0, len - 1)) {
            substores.set(stores[2 * i] as SubStoreNum, stores[2 * i + 1]);
        }
        return { ty, substores };
    } else if (ty == RecordType.CREATE_PAGE) {
        const record: Partial<CreatePage> = {};
        [
            record.ty,
            record.namespace,
            record.pageNum,
            record.where,
        ] = string.unpack(CREATE_PAGE_FMT, str);
        return record as CreatePage;
    } else if (ty == RecordType.DELETE_PAGE) {
        const record: Partial<DeletePage> = {};
        [
            record.ty,
            record.namespace,
            record.pageNum,
            record.where,
        ] = string.unpack(DELETE_PAGE_FMT, str);
        return record as DeletePage;
    } else if (ty == RecordType.CREATE_SUB_STORE) {
        const record: Partial<CreateSubStore> = {};
        [
            record.ty,
            record.where,
        ] = string.unpack(CREATE_SUB_STORE_FMT, str);
        return record as CreateSubStore;
    } else if (ty == RecordType.DELETE_SUB_STORE) {
        const record: Partial<DeleteSubStore> = {};
        [
            record.ty,
            record.where,
        ] = string.unpack(DELETE_SUB_STORE_FMT, str);
        return record as DeleteSubStore;
    } else if (ty == RecordType.MOVE_PAGE) {
        const record: Partial<MovePage> = {};
        [
            record.ty,
            record.namespace,
            record.pageNum,
            record.from,
            record.to,
        ] = string.unpack(MOVE_PAGE_FMT, str);
        return record as MovePage;
    } else {
        throw new Error("Invalid record type " + ty);
    }
}

/**
 * A simple log for state transactions on an indexed store.
 *
 * In contrast to a full write-ahead log, it:
 * - Has no CLRs (except delete which are used as a "CLR" for undoing create).
 * - Doesn't require writing the dirty LSN in the target page.
 * - Supports immediately deleting target pages.
 * - Only has one active transaction at a time.
 * - Limits every transaction to a single record.
 * - Commits are externally made (by changing the pages in the sub-stores.)
 */
export class IndexLog {
    /** The underlying log to store records into. */
    private log: RecordLog;

    /** How many pages each sub-store has allocated. */
    public usages: LuaMap<SubStoreNum, number>;

    private numUsages;

    /** The last procedure on the log, if any, which may need a redo. */
    public lastProcedure?: ProcedureRecord;

    public constructor(log: RecordLog) {
        this.log = log;
        this.usages = new LuaMap();
        this.numUsages = 0;

        if (log.isEmpty()) {
            // Initialize the log with an empty checkpoint.
            log.flushToPoint(log.appendRecord(encodeRecord({
                ty: RecordType.CHECKPOINT,
                substores: new LuaMap(),
            })));
        }

        // Repeat history by walking the log.
        let lsn = log.getStart();
        while (lsn != log.getEnd()) {
            const [recordStr, nextLsn] = log.getRecord(lsn);
            const r = decodeRecord(recordStr);
            this.updateState(r);
            lsn = nextLsn;
        }
    }

    /** Updates the usages given a record describing a modification to them. */
    private updateState(record: IndexRecord) {
        if (record.ty == RecordType.CHECKPOINT) {
            this.usages = record.substores;
            this.numUsages = 0;
            for (const [_] of this.usages) { this.numUsages++; }
            this.lastProcedure = undefined;
        } else if (record.ty == RecordType.CREATE_PAGE) {
            const usage = assert(this.usages.get(record.where));
            this.usages.set(record.where, usage + 1);
            this.lastProcedure = record;
        } else if (record.ty == RecordType.DELETE_PAGE) {
            const usage = assert(this.usages.get(record.where));
            this.usages.set(record.where, usage - 1);
            this.lastProcedure = record;
        } else if (record.ty == RecordType.CREATE_SUB_STORE) {
            this.usages.set(record.where, 0);
            this.numUsages++;
            this.lastProcedure = record;
        } else if (record.ty == RecordType.DELETE_SUB_STORE) {
            this.usages.delete(record.where);
            this.numUsages--;
            this.lastProcedure = record;
        } else if (record.ty == RecordType.MOVE_PAGE) {
            const sourceUsage = assert(this.usages.get(record.from));
            this.usages.set(record.from, sourceUsage - 1);
            const targetUsage = assert(this.usages.get(record.to));
            this.usages.set(record.to, targetUsage + 1);
            this.lastProcedure = record;
        }
    }

    public registerProcedure(record: ProcedureRecord) {
        this.updateState(record);
        this.log.flushToPoint(this.log.appendRecord(encodeRecord(record)));
    }

    public writeCheckpointIfFull() {
        const logSize = this.log.getEnd() - this.log.getStart();
        if (logSize / this.numUsages > 64 && this.log.getNumPages() > 2) {
            this.writeCheckpoint();
        }
    }

    public writeCheckpoint() {
        const lsn = this.log.appendRecord(encodeRecord({
            ty: RecordType.CHECKPOINT,
            substores: this.usages,
        }));
        this.log.flushToPoint(lsn);
        this.log.trimToPoint(lsn);
    }
}
