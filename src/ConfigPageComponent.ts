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

export class ConfigEntryId {
    public readonly namespace: Namespace;
    public readonly key: number;
    public constructor(namespace: Namespace, key: number) {
        this.namespace = namespace;
        this.key = key;
    }
}

/** A component for storing a configuration value in a page. */
export class ConfigEntryComponent<T> {
    public readonly namespace: Namespace;
    private key: number;
    private fmt: string;
    private defaultValue: T;

    public constructor(
        id: ConfigEntryId,
        fmt: string,
        defaultValue: T,
    ) {
        this.namespace = id.namespace;
        this.key = id.key;
        this.fmt = fmt;
        this.defaultValue = defaultValue;
    }

    public deserializeObj(n: Namespace, s?: string): ConfigObj | undefined {
        if (n != this.namespace) { return; }
        const entries = new LuaMap<number, string>();
        if (s) {
            let pos = 1;
            while (pos <= s.length) {
                const [key, value, nxtPos] = string.unpack(ENTRY_FMT, s, pos);
                entries.set(key, value);
                pos = nxtPos;
            }
        }
        return new ConfigObj(entries);
    }

    public deserializeEv(n: Namespace, s: string): SetEntryEvent | undefined {
        if (n != this.namespace) { return; }
        if (s.length == CONFIG_KEY_BYTES) {
            const [key, value] = string.unpack(SET_ENTRY_FMT1, s);
            return new SetEntryEvent(key, value);
        } else {
            const [key] = string.unpack(SET_ENTRY_FMT2, s);
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
