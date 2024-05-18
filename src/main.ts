import * as expect from "cc/expect";
import { KvStore } from "./core/KvStore";
import { breakDeadlocks } from "./lock/Lock";

/**
 * Opens a database in a directory.
 * @param dir - The directory to store in.
 */
export function open(this: void, dir: string): KvStore {
    expect(1, dir, "string");
    return new KvStore(dir);
}

export function daemon() {
    while (true) {
        sleep(5);
        breakDeadlocks();
    }
}
