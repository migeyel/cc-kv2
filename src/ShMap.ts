/** A map to make sure a shared parent with live children can't get dropped. */
const parents = setmetatable(
    new LuaMap<object, object>(),
    { __mode: "k" },
);

/** A simple map for shared values over a weak table. */
export class ShMap<K extends AnyNotNil, V extends object | null | undefined> {
    private map = setmetatable(new LuaMap<K, V>(), { __mode: "v" });

    /** Gets a value from the map or computes it from a closure. */
    public getOr(parent: object, key: K, fn: () => V): V {
        const out = this.map.get(key);
        if (out) {
            return out;
        } else {
            const newOut = fn();
            this.map.set(key, newOut);
            if (newOut) { parents.set(newOut, parent); }
            return newOut;
        }
    }
}
