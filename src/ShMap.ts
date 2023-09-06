/** A simple map for shared values over a weak table. */
export class ShMap<K extends AnyNotNil, V extends object> {
    private map = setmetatable(new LuaMap<K, V>(), { __mode: "v" });

    /** Gets a value from the map or computes it from a closure. */
    public getOr(key: K, fn: () => V): V {
        const out = this.map.get(key);
        if (out) {
            return out;
        } else {
            const newOut = fn();
            this.map.set(key, newOut);
            return newOut;
        }
    }
}
