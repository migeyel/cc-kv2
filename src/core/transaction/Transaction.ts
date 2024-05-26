import {
    TxId,
    TxCollection,
} from "../txStore/LogStore";
import { KvLockManager } from "./KvLockManager";
import * as expect from "cc/expect";
import { LockedResource, LockHolder } from "./Lock";
import { SetEntryAct, SetEntryConfig } from "../SetEntryConfig";
import { KvPair } from "../btree/Node";

export class Transaction {
    private id: TxId;
    private cl: TxCollection;
    private config: SetEntryConfig;
    private kvlm: KvLockManager;
    private onClose: () => void;
    private active = true;

    public readonly holder = new LockHolder();

    public constructor(
        id: TxId,
        cl: TxCollection,
        config: SetEntryConfig,
        kvlm: KvLockManager,
        onClose: () => void,
    ) {
        this.id = id;
        this.cl = cl;
        this.config = config;
        this.kvlm = kvlm;
        this.onClose = onClose;
    }

    /**
     * Gets a value.
     * @param key - The key to get.
     * @returns The value matching the key.
     */
    public get(key: string): string | undefined {
        assert(this.active, "can't operate on an inactive transaction");
        this.kvlm.acquireGet(key, this.holder);
        const [_, pair] = this.config.btree.search(this.cl, key);
        if (pair && pair.key == key) { return pair.value; }
    }

    /**
     * Returns the next key-value pair starting from a key.
     * @param key - A key to fetch the next pair from.
     * @returns A key-value pair coming after the given key, if any.
     */
    public next(key?: string): LuaMultiReturn<[string, string] | []> {
        assert(this.active, "can't operate on an inactive transaction");
        const nextSmallestKey = key ? key + "\0" : "";
        this.kvlm.acquireNext(this.cl, nextSmallestKey, this.holder);
        const [_, pair] = this.config.btree.search(this.cl, nextSmallestKey);
        if (pair) {
            return $multi(pair.key, pair.value);
        } else {
            return $multi();
        }
    }

    /** Finds the previous and next key-value pairs from a key. */
    public find(
        key: string,
    ): LuaMultiReturn<[KvPair | undefined, KvPair | undefined]> {
        assert(this.active, "can't operate on an inactive transaction");
        this.kvlm.acquireNext(this.cl, key, this.holder);
        this.kvlm.acquirePrev(this.cl, key, this.holder);
        return this.config.btree.search(this.cl, key);
    }

    /**
     * Iterates over keys.
     * @param start - A starting key, or nothing to start from the smallest.
     * @returns An iterator over all keys starting from the argument.
     */
    public iter(
        start?: string,
    ): LuaIterable<LuaMultiReturn<[string, string] | []>, Transaction> {
        expect(1, start, "string", "nil");
        // @ts-expect-error: Just go for it.
        return $multi(
            this.next,
            this,
            start,
        );
    }

    /**
     * Sets a key.
     * @param key - The key to set.
     * @param value - The value to set to.
     */
    public set(key: string, value: string): void {
        assert(this.active, "can't operate on an inactive transaction");
        this.kvlm.acquireSet(this.cl, key, this.holder);
        this.cl.doAct(this.id, <SetEntryAct>{ key, value });
    }

    /**
     * Deletes a key.
     * @param key - The key to delete.
     */
    public delete(key: string): void {
        assert(this.active, "can't operate on an inactive transaction");
        this.kvlm.acquireDelete(this.cl, key, this.holder);
        this.cl.doAct(this.id, <SetEntryAct>{ key });
    }

    /**
     * Commits the transaction.
     * @returns The set of released resources.
     */
    public commit(): LuaSet<LockedResource> {
        assert(this.active, "can't operate on an inactive transaction");
        this.cl.commit(this.id);
        const out = this.holder.releaseAll();
        this.active = false;
        this.onClose();
        return out;
    }

    /**
     * Rolls the transaction back.
     * @returns The set of released resources.
     */
    public rollback(): LuaSet<LockedResource> {
        if (!this.active) { return new LuaSet(); }
        this.cl.rollback(this.id);
        const out = this.holder.releaseAll();
        this.active = false;
        this.onClose();
        return out;
    }
}
