import * as expect from "cc/expect";
import { KvStore } from "./Api";

/**
 * Opens a database in a directory.
 * @param dir - The directory to store in.
 */
export function open(this: void, dir: string): KvStore {
    expect(1, dir, "string");
    return new KvStore(dir);
}
