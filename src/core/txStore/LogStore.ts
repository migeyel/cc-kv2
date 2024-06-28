import { CacheMap, ICacheable } from "../CacheMap";
import { RecordLog } from "../RecordLog";
import { ShMap } from "../ShMap";
import {
    IPage,
    IPageStore,
    IStoreCollection,
    Namespace,
    PageNum,
    PageSize,
} from "../store/IPageStore";
import {
    LSN_FMT,
    NAMESPACE_FMT,
    PAGE_FMT,
    PageUpdateType,
    RecordType,
    updatesOnEmpty,
} from "./LogRecord/types";
import * as LogRecord from "./LogRecord/LogRecord";
import * as EventSubRecord from "./LogRecord/EventSubRecord";
import * as CheckpointRecord from "./LogRecord/CheckpointRecord";
import * as ActRecord from "./LogRecord/ActRecord";
import * as ClrRecord from "./LogRecord/ClrRecord";
import * as CommitRecord from "./LogRecord/CommitRecord";
import { TtEntry, DptEntry } from "./LogRecord/CheckpointRecord";
import { ActState, State } from "./State";

export type TxId = number & { readonly __brand: unique symbol }

/** A page object that can be transacted by using events.  */
export interface IObj<E extends IEvent> {
    /** Applies an event to the object. */
    apply(event: E): void;

    /** Whether the object is empty. */
    isEmpty(): boolean;

    /** Serializes the object. Only called when `isEmpty()` returns false. */
    serialize(): string;
}

/** An event that transacts a page object. */
export interface IEvent {
    serialize(): string;
}

/** A configuration for a collection. */
export interface IConfig {
    /** Deserializes an object. */
    deserializeObj(namespace: Namespace, str?: string): IObj<IEvent>;

    /** Deserializes an event. */
    deserializeEv(namespace: Namespace, str: string): IEvent;

    /** Applies one or more events to perform a given act. */
    doAct(
        act: object,
        collection: TxCollection,
    ): LuaMultiReturn<[string, object | undefined]>;

    /** Applies one or more events to logically undo a given act. */
    undoAct(
        undoInfo: string,
        collection: TxCollection,
        events: EventSubRecord.Record[],
        eventOffset: number,
        extraContext?: object,
    ): object | undefined;

    /** The cache for pages in the collection. */
    readonly cache: CacheMap<CacheKey, AnyTxPage>;
}

export type CacheKey = string & { readonly __brand: unique symbol };

/** A TxPage object with any inner object type. */
export type AnyTxPage = TxPage<IObj<IEvent>, IEvent>;

const CK_FMT = PAGE_FMT + NAMESPACE_FMT;

/** Returns a cache key for the namespace and page number. */
function cacheKey(namespace: Namespace, pageNum: PageNum): CacheKey {
    return string.pack(CK_FMT, pageNum, namespace) as CacheKey;
}

export class TxPage<T extends IObj<E>, E extends IEvent> implements ICacheable {
    private state: State;
    private page: IPage;
    private cache: CacheMap<CacheKey, AnyTxPage>;

    public readonly namespace: Namespace;
    public readonly pageNum: PageNum;

    public config: IConfig;
    public pageSize: PageSize;

    /**
     * The LSN of the last act that changed this page.
     *
     * The value is set to 0 if the page was reloaded from disk after being deleted.
     * Because event application is idempotent whenever starting from an empty page, the
     * LSN doesn't need to be recorded.
     */
    public pageLsn = 0;

    /** The deserialized page object. */
    public obj: T;

    /** Whether the page exists on disk. */
    private pageExists: boolean;

    public constructor(
        state: State,
        namespace: Namespace,
        page: IPage,
        config: IConfig,
        cache: CacheMap<CacheKey, AnyTxPage>,
    ) {
        this.pageSize = page.pageSize;
        this.state = state;
        this.page = page;
        this.config = config;
        this.cache = cache;
        this.namespace = namespace;
        this.pageNum = page.pageNum;

        const pageStr = this.page.read();
        if (pageStr) {
            const [pageLsn, pos] = string.unpack("<" + LSN_FMT, pageStr);
            this.pageLsn = pageLsn;
            this.obj = this.config.deserializeObj(
                this.namespace,
                string.sub(pageStr, pos),
            ) as T;
            this.pageExists = true;
        } else {
            this.obj = this.config.deserializeObj(this.namespace) as T;
            this.pageExists = false;
        }
    }

    private cacheKey(): CacheKey {
        return cacheKey(this.namespace, this.pageNum);
    }

    /** Bumps the page in the cache queue, putting it in if it wasn't there. */
    private bump() {
        this.cache.getOr(this.cacheKey(), () => this);
    }

    /** Pins the page and adds it to the act pinned page list. */
    private pin() {
        this.bump();
        const [as] = assert(this.state.actState, "can't pin without act");
        as.pinnedPages.add(this);
        this.cache.pin(this.cacheKey());
    }

    /** Unpins the page and removes it from the act pinned page list. */
    private unpin() {
        const [as] = assert(this.state.actState, "can't unpin without act");
        as.pinnedPages.delete(this);
        this.cache.unpin(this.cacheKey());
    }

    public evict(): void {
        const dptInfo = this.state.getDptEntry(
            this.namespace,
            this.pageNum,
        );

        // If the page isn't dirty it needs no flushing.
        if (!dptInfo) { return; }

        // WAL policy
        this.state.log.flushToPoint(this.pageLsn);

        if (this.obj.isEmpty()) {
            if (this.pageExists) {
                this.page.delete();
                this.pageExists = false;
            } else {
                // Nothing to do.
            }
        } else {
            const lsnStr = string.pack("<" + LSN_FMT, this.pageLsn);
            if (this.pageExists) {
                this.page.write(lsnStr + this.obj.serialize());
            } else {
                this.page.create(lsnStr + this.obj.serialize());
                this.pageExists = true;
            }
        }

        // The page is no longer dirty.
        this.state.deleteDptEntry(this.namespace, this.pageNum);
    }

    /** Applies end-of-act state changes to the page. */
    public closeAct(actLsn: number) {
        const dptEntry = this.state.getDptEntry(this.namespace, this.pageNum);
        if (!dptEntry) { this.state.setDptEntry(this.namespace, this.pageNum, actLsn); }
        this.pageLsn = actLsn;
        this.unpin();
    }

    /**
     * Applies a new event to the page object, appending a new entry to the act.
     * This also pins the page.
     */
    public doEvent(event: E): void {
        this.pin();

        // Apply and get what change was made to the page state.
        const wasEmpty = this.obj.isEmpty();
        this.obj.apply(event);
        const isEmpty = this.obj.isEmpty();
        let updateType: PageUpdateType;
        if (isEmpty) {
            if (wasEmpty) {
                updateType = PageUpdateType.EMPTY;
            } else {
                updateType = PageUpdateType.DELETED;
            }
        } else {
            if (wasEmpty) {
                updateType = PageUpdateType.CREATED;
            } else {
                updateType = PageUpdateType.ALTERED;
            }
        }

        assert(this.state.actState).pushEvent({
            updateType,
            namespace: this.namespace,
            pageNum: this.pageNum,
            event: event.serialize(),
        });
    }

    /** Redoes an event from a record. This also pins the page. */
    public redoEvent(record: EventSubRecord.Record): void {
        if (this.obj.isEmpty() == updatesOnEmpty[record.updateType]) {
            this.pin();
            this.obj.apply(this.config.deserializeEv(
                this.namespace,
                record.event,
            ) as E);
        }
    }
}

export class TxStore<
    T extends IObj<E>,
    E extends IEvent,
> {
    private state: State;
    private store: IPageStore<IPage>;
    private namespace: Namespace;
    private map: ShMap<AnyTxPage, TxStore<IObj<IEvent>, IEvent>>;

    public config: IConfig;
    public pageSize: PageSize;

    public constructor(
        state: State,
        namespace: Namespace,
        map: ShMap<AnyTxPage, TxStore<IObj<IEvent>, IEvent>>,
        store: IPageStore<IPage>,
        config: IConfig,
    ) {
        this.pageSize = store.pageSize;
        this.state = state;
        this.namespace = namespace;
        this.map = map;
        this.store = store;
        this.config = config;
    }

    public getPage(pageNum: PageNum): TxPage<T, E> {
        const key = cacheKey(this.namespace, pageNum);
        return this.map.getPage(this.namespace, pageNum, () => {
            return this.config.cache.getOr(key, () => {
                return new TxPage(
                    this.state,
                    this.namespace,
                    this.store.getPage(pageNum),
                    this.config,
                    this.config.cache,
                );
            });
        }) as TxPage<T, E>;
    }
}

export class TxCollection {
    private state: State;
    private collection: IStoreCollection<IPage, IPageStore<IPage>>;
    private config: IConfig;
    private map = new ShMap<AnyTxPage, TxStore<IObj<IEvent>, IEvent>>();
    private lastCpLsn!: number;
    private autoCpLimit: number;
    private autoFlushLimitSize?: number;

    public readonly pageSize: PageSize;

    public constructor(
        log: RecordLog,
        collection: IStoreCollection<IPage, IPageStore<IPage>>,
        config: IConfig,
        autoCpLimit: number,
        autoFlushLimitSize?: number,
    ) {
        this.autoCpLimit = autoCpLimit;
        this.autoFlushLimitSize = autoFlushLimitSize;
        this.pageSize = collection.pageSize;
        this.config = config;
        this.collection = collection;
        this.state = new State(
            new LuaMap(),
            new LuaMap(),
            log,
        );

        if (log.isEmpty()) { this.checkpoint(); }

        this.recover();
    }

    /** Gets a store, casting it to the given stored object type. */
    public getStoreCast<T extends IObj<E>, E extends IEvent>(
        namespace: Namespace,
    ): TxStore<T, E> {
        return this.map.getStore(namespace, () => new TxStore(
            this.state,
            namespace,
            this.map,
            this.collection.getStore(namespace),
            this.config,
        )) as unknown as TxStore<T, E>;
    }

    /** Commits a transaction. */
    public commit(txId: TxId) {
        const lsn = this.state.log.appendRecord(CommitRecord.serialize({
            ty: RecordType.COMMIT,
            txId,
        }));
        this.state.log.flushToPoint(lsn);
        this.state.deleteTtEntry(txId);
    }

    /** Applies end-of-act changes to the collection state. */
    private closeAct(txId: TxId, actLsn: number, undoNxtLsn: number) {
        const ttEntry = this.state.getTtEntry(txId);
        if (ttEntry.firstLsn == 0) { ttEntry.firstLsn = actLsn; }
        ttEntry.lastLsn = actLsn;
        ttEntry.undoNxtLsn = undoNxtLsn;

        for (const page of assert(this.state.actState).pinnedPages) {
            page.closeAct(actLsn);
        }

        this.state.actState = undefined;
    }

    /** Performs an act logically, appending a new record to the log. */
    public doAct(txId: TxId, act: object): object | undefined {
        assert(this.state.actState == undefined);
        this.state.actState = new ActState();

        const [undoInfo, result] = this.config.doAct(act, this);

        const ttEntry = this.state.getTtEntry(txId);
        const actLsn = this.state.log.appendRecord(ActRecord.serialize({
            ty: RecordType.ACT,
            txId,
            prevLsn: ttEntry.lastLsn,
            undoInfo,
            events: assert(this.state.actState).events,
        }));

        this.closeAct(txId, actLsn, actLsn);
        this.autoCheckpoint();

        return result;
    }

    /** Redoes an act or CLR phisycally, given a log record. */
    private redoAct(
        actLsn: number,
        record: ActRecord.Record | ClrRecord.Record,
    ) {
        assert(this.state.actState == undefined);
        this.state.actState = new ActState();

        for (const event of record.events) {
            const page = this.getStoreCast(event.namespace)
                .getPage(event.pageNum);
            if (page.pageLsn < actLsn) { page.redoEvent(event); }
        }

        this.closeAct(record.txId, actLsn, actLsn);
    }

    /** Undoes an act logically, appending a new CLR to the log. */
    private undoAct(record: ActRecord.Record) {
        assert(this.state.actState == undefined);
        this.state.actState = new ActState();

        this.config.undoAct(record.undoInfo, this, record.events, 0);

        const ttEntry = this.state.getTtEntry(record.txId);
        const clrLsn = this.state.log.appendRecord(ClrRecord.serialize({
            ty: RecordType.CLR,
            txId: record.txId,
            prevLsn: ttEntry.lastLsn,
            undoNxtLsn: record.prevLsn,
            events: assert(this.state.actState).events,
        }));

        this.closeAct(record.txId, clrLsn, record.prevLsn);
        this.autoCheckpoint();
    }

    /** Rolls a transaction back. */
    public rollback(txId: TxId) {
        const ttEntry = this.state.tt.get(txId);
        if (!ttEntry) { return; }

        while (ttEntry.undoNxtLsn != 0) {
            const recordStr = this.state.log.getRecord(ttEntry.undoNxtLsn)[0];
            const record = LogRecord.deserialize(recordStr);
            if (record.ty == RecordType.ACT) {
                this.undoAct(record);
            } else if (record.ty == RecordType.CLR) {
                ttEntry.undoNxtLsn = record.undoNxtLsn;
            } else {
                throw new Error("Invalid undo record " + record.ty);
            }
        }

        this.state.deleteTtEntry(txId);
    }

    /** Creates a state checkpoint. */
    public checkpoint(flushLimitSize?: number) {
        assert(!this.state.actState);

        let trimLsn = this.state.log.getEnd();
        const tt = <TtEntry[]>[];
        for (const [_, t] of this.state.tt) {
            tt.push(t);
            if (t.firstLsn > 0) { trimLsn = math.min(trimLsn, t.firstLsn); }
        }

        const flushLsn = flushLimitSize ?
            math.min(trimLsn, this.state.log.getEnd() - flushLimitSize) :
            undefined;

        const dpt = <DptEntry[]>[];
        for (const [namespace, dps] of this.state.dpt) {
            for (const [pageNum, dp] of dps) {
                if (flushLsn && dp.recLsn < flushLsn) {
                    this.getStoreCast(namespace)
                        .getPage(pageNum)
                        .evict();
                } else {
                    trimLsn = math.min(trimLsn, dp.recLsn);
                    dpt.push(dp);
                }
            }
        }

        this.lastCpLsn = this.state.log.appendRecord(LogRecord.serialize({
            ty: RecordType.CHECKPOINT,
            dpt,
            tt,
        }));

        this.state.log.flushToPoint(this.lastCpLsn);
        this.state.log.trimToPoint(trimLsn);
    }

    private autoCheckpoint() {
        const logCpNum = this.state.log.getEnd() - this.lastCpLsn;
        if (logCpNum < this.autoCpLimit) { return; }
        this.checkpoint(this.autoFlushLimitSize);
    }

    private throttle(t0: number): number {
        if (os.epoch("utc") - t0 > 100) {
            os.queueEvent("cckv2_recovery_throttle");
            os.pullEvent("cckv2_recovery_throttle");
            return os.epoch("utc");
        } else {
            return t0;
        }
    }

    /** Performs state recovery from the log. */
    private recover() {
        const log = this.state.log;
        let t0 = os.epoch("utc");

        // Get where the last checkpoint is.
        let walkLsn = log.getStart();
        let lastCpLsn;
        while (walkLsn < log.getEnd()) {
            const [recordStr, nextLsn] = log.getRecord(walkLsn);
            const recordTy = LogRecord.getType(recordStr);
            if (recordTy == RecordType.CHECKPOINT) { lastCpLsn = walkLsn; }
            walkLsn = nextLsn;
            t0 = this.throttle(t0);
        }

        this.lastCpLsn = assert(lastCpLsn);

        // Read checkpoint.
        const lastCp = CheckpointRecord.deserialize(
            log.getRecord(assert(lastCpLsn))[0],
        );

        let analysisLsn = assert(lastCpLsn);
        for (const dp of lastCp.dpt) {
            analysisLsn = math.min(analysisLsn, dp.recLsn);
            this.state.setDptEntry(dp.namespace, dp.pageNum, dp.recLsn);
        }

        for (const t of lastCp.tt) {
            const info = this.state.getTtEntry(t.id);
            info.firstLsn = t.firstLsn;
            info.lastLsn = t.lastLsn;
            info.undoNxtLsn = t.undoNxtLsn;
        }

        // Analysis + Redo pass.
        while (analysisLsn < log.getEnd()) {
            const [recordStr, nextLsn] = log.getRecord(analysisLsn);
            const record = LogRecord.deserialize(recordStr);
            if (record.ty == RecordType.ACT || record.ty == RecordType.CLR) {
                this.redoAct(analysisLsn, record);
            } else if (record.ty == RecordType.COMMIT) {
                this.state.deleteTtEntry(record.txId);
            }
            analysisLsn = nextLsn;
            t0 = this.throttle(t0);
        }

        // Undo pass.
        while (true) {
            let nextLsn = 0;
            let nextTtEntry: TtEntry | undefined;
            for (const [_, ttEntry] of this.state.tt) {
                // Inefficient max finding
                // TODO: Use a heap or something else
                if (ttEntry.undoNxtLsn > nextLsn) {
                    nextLsn = ttEntry.undoNxtLsn;
                    nextTtEntry = ttEntry;
                }
            }
            if (nextTtEntry && nextLsn != 0) {
                const recordStr = this.state.log.getRecord(nextLsn)[0];
                const record = LogRecord.deserialize(recordStr);
                if (record.ty == RecordType.ACT) {
                    this.undoAct(record);
                } else if (record.ty == RecordType.CLR) {
                    nextTtEntry.undoNxtLsn = record.undoNxtLsn;
                } else {
                    throw new Error("Invalid undo record " + record.ty);
                }
                t0 = this.throttle(t0);
            } else {
                break;
            }
        }

        // Clear the TT and write a new checkpoint.
        this.state.tt = new LuaMap();
        this.checkpoint(0);
    }
}
