import { CacheMap } from "../../CacheMap";
import {
    AnyTxPage,
    CacheKey,
    IConfig,
    IEvent,
    IObj,
    TxCollection,
} from "../../txStore/LogStore";
import { Namespace, PageNum } from "../IPageStore";
import {
    SUBSTORE_FMT,
    SUBSTORE_BYTELEN,
    INDEX_POS_FMT,
    SetIndexEntryEvent,
    IndexObj,
} from "./IndexObj";

const GLOBAL_POS_FMT = "<I6";

export type SetIndexEntryAct = {
    pos: number,
    val: number,
};

export class IndexConfig implements IConfig {
    public readonly cache: CacheMap<CacheKey, AnyTxPage>;

    public constructor(cache: CacheMap<CacheKey, AnyTxPage>) {
        this.cache = cache;
    }

    public deserializeObj(
        _namespace: Namespace,
        str?: string | undefined,
    ): IObj<IEvent> {
        return str ? IndexObj.deserialize(str) : new IndexObj(new LuaMap());
    }

    public deserializeEv(_namespace: Namespace, str: string): IEvent {
        return SetIndexEntryEvent.deserialize(str);
    }

    public doAct(
        act: SetIndexEntryAct,
        collection: TxCollection,
    ): LuaMultiReturn<[string, object | undefined]> {
        const entriesPerPage = math.floor(collection.pageSize / SUBSTORE_BYTELEN);
        const entryPos = act.pos % entriesPerPage;
        const pageNum = (act.pos - entryPos) / entriesPerPage;
        const page =  collection
            .getStoreCast<IndexObj, SetIndexEntryEvent>(0 as Namespace)
            .getPage(pageNum as PageNum);
        const oldVal = page.obj.substores.get(entryPos) || 0;
        page.doEvent(new SetIndexEntryEvent(entryPos, act.val));
        const undoInfo = string.pack(GLOBAL_POS_FMT + SUBSTORE_FMT, act.pos, oldVal);
        return $multi(undoInfo, undefined);
    }

    public undoAct(undoInfo: string, collection: TxCollection): object | undefined {
        const entriesPerPage = math.floor(collection.pageSize / SUBSTORE_BYTELEN);
        const [pos, oldVal] = string.unpack(GLOBAL_POS_FMT + INDEX_POS_FMT, undoInfo);
        const entryPos = pos % entriesPerPage;
        const pageNum = (pos - entryPos) / entriesPerPage;
        const page =  collection
            .getStoreCast<IndexObj, SetIndexEntryEvent>(0 as Namespace)
            .getPage(pageNum as PageNum);
        page.doEvent(new SetIndexEntryEvent(entryPos, oldVal));
        return undefined;
    }
}
