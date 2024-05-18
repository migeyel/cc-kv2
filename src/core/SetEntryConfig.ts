import { CacheMap } from "./CacheMap";
import { BTreeComponent } from "./btree/Node";
import { Namespace } from "./store/IPageStore";
import {
    AnyTxPage,
    CacheKey,
    IConfig,
    IEvent,
    IObj, TxCollection,
} from "./txStore/LogStore";

export type SetEntryAct = {
    key: string,
    value?: string,
};

export class SetEntryConfig implements IConfig {
    public readonly cache: CacheMap<CacheKey, AnyTxPage>;
    public readonly btree: BTreeComponent;

    public constructor(
        cache: CacheMap<CacheKey, AnyTxPage>,
        btree: BTreeComponent,
    ) {
        this.cache = cache;
        this.btree = btree;
    }

    public deserializeObj(ns: Namespace, str?: string): IObj<IEvent> {
        return assert(this.btree.deserializeObj(ns, str));
    }

    public deserializeEv(ns: Namespace, str: string): IEvent {
        return assert(this.btree.deserializeEv(ns, str));
    }

    public doAct(
        act: SetEntryAct,
        collection: TxCollection,
    ): LuaMultiReturn<[string, undefined]> {
        if (act.value) {
            const oldValue = this.btree.insert(collection, act.key, act.value);
            if (oldValue) {
                return $multi(
                    string.pack("<s4s4", act.key, oldValue),
                    undefined,
                );
            } else {
                return $multi(
                    string.pack("<s4", act.key),
                    undefined,
                );
            }
        } else {
            const oldValue = this.btree.delete(collection, act.key);
            if (oldValue) {
                return $multi(
                    string.pack("<s4s4", act.key, oldValue),
                    undefined,
                );
            } else {
                return $multi(
                    string.pack("<s4", act.key),
                    undefined,
                );
            }
        }
    }

    public undoAct(undoInfo: string, collection: TxCollection): undefined {
        const [key, pos] = string.unpack("<s4", undoInfo);
        if (pos <= undoInfo.length) {
            const [value] = string.unpack("<s4", undoInfo, pos);
            this.btree.insert(collection, key, value);
        } else {
            this.btree.delete(collection, key);
        }
    }
}
