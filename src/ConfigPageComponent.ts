import { Namespace, PageNum } from "./store/IPageStore";
import { IEvent, IObj, TxCollection } from "./txStore/LogStore";
import { uIntLenBytes } from "./util";

const MAX_CONFIG_KEY_VALUE = 255;
const CONFIG_KEY_BYTES = uIntLenBytes(MAX_CONFIG_KEY_VALUE);

const MAX_CONFIG_ENTRY_SIZE = 255;
const CONFIG_ENTRY_LEN_BYTES = uIntLenBytes(MAX_CONFIG_ENTRY_SIZE);

const SET_ENTRY_FMT1 = "<I" + CONFIG_KEY_BYTES + "s" + CONFIG_ENTRY_LEN_BYTES;
const SET_ENTRY_FMT2 = "<I" + CONFIG_KEY_BYTES;

export class SetEntryEvent implements IEvent {
    public readonly key: number;
    public readonly value?: string;

    public constructor(key: number, value?: string) {
        this.key = key;
        this.value = value;
    }

    public serialize(): string {
        if (this.value) {
            return string.pack(SET_ENTRY_FMT1, this.key, this.value);
        } else {
            return string.pack(SET_ENTRY_FMT2, this.key);
        }
    }
}

const ENTRY_FMT = "<I" + CONFIG_ENTRY_LEN_BYTES + "s" + CONFIG_ENTRY_LEN_BYTES;

export class ConfigObj implements IObj<SetEntryEvent> {
    public entries: LuaMap<number, string>;

    public constructor(entries: LuaMap<number, string>) {
        this.entries = entries;
    }

    public apply(event: SetEntryEvent): void {
        if (event.value) {
            this.entries.set(event.key, event.value);
        } else {
            this.entries.delete(event.key);
        }
    }

    public isEmpty(): boolean {
        return next(this.entries)[0] == undefined;
    }

    public serialize(): string {
        const out = [];
        for (const [k, v] of this.entries) {
            out.push(string.pack(ENTRY_FMT, k, v));
        }
        return table.concat(out);
    }
}

/** A component for storing a configuration value in a page. */
export class ConfigEntryComponent<T> {
    public readonly namespace: Namespace;
    private key: number;
    private fmt: string;
    private defaultValue: T;

    public constructor(
        namespace: Namespace,
        key: number,
        fmt: string,
        defaultValue: T,
    ) {
        this.namespace = namespace;
        this.key = key;
        this.fmt = fmt;
        this.defaultValue = defaultValue;
    }

    public deserializeObj(namespace: Namespace, str?: string): ConfigObj {
        assert(namespace == this.namespace);
        const entries = new LuaMap<number, string>();
        if (str) {
            let pos = 1;
            while (pos <= str.length) {
                const [key, value, nxtPos] = string.unpack(ENTRY_FMT, str, pos);
                entries.set(key, value);
                pos = nxtPos;
            }
        }
        return new ConfigObj(entries);
    }

    public deserializeEv(namespace: Namespace, str: string): SetEntryEvent {
        assert(namespace == this.namespace);
        if (str.length == CONFIG_KEY_BYTES) {
            const [key, value] = string.unpack(SET_ENTRY_FMT1, str);
            return new SetEntryEvent(key, value);
        } else {
            const [key] = string.unpack(SET_ENTRY_FMT2, str);
            return new SetEntryEvent(key);
        }
    }

    public get(cl: TxCollection): T {
        const str = cl
            .getStoreCast<ConfigObj, SetEntryEvent>(this.namespace)
            .getPage(0 as PageNum)
            .obj
            .entries
            .get(this.key);
        if (!str) { return this.defaultValue; }
        return string.unpack(this.fmt, str)[0];
    }

    public set(cl: TxCollection, value: T): void {
        const str = string.pack(this.fmt, value);
        cl.getStoreCast<ConfigObj, SetEntryEvent>(this.namespace)
            .getPage(0 as PageNum)
            .doEvent(new SetEntryEvent(this.key, str));
    }
}