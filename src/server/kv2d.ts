import { DirKvStore } from "../core/KvStore";
import { isUuid4 } from "../common/uid";
import { TxManager } from "./TxManager";

export function main(path: string) {
    // Load disk drives.
    const seenDisks = new LuaSet<number>();
    const dataDirs = [];
    for (const drive of peripheral.find("drive") as LuaMultiReturn<DrivePeripheral[]>) {
        const id = drive.getDiskID();
        if (id && !seenDisks.has(id)) {
            seenDisks.add(id);
            const mountPath = drive.getMountPath();
            for (const name of fs.list(mountPath)) {
                if (isUuid4(name)) {
                    dataDirs.push(fs.combine(mountPath, name));
                }
            }
        }
    }

    // Load the database.
    const db = new DirKvStore(path, dataDirs);
    const txm = new TxManager(db);
    db.open();

    // Run the manager main loop.
    txm.mainLoop();
}
