import { DirKvStore } from "./KvStore";
import { isUuid4, uuid4 } from "./common/uid";
import { TxWorker } from "./transaction/TxWorker";
import { isRequest, Request, Response } from "./apis/userApi";
import { WorkerResume, WorkerYield } from "./transaction/txWorkerApi";
import {
    breakDeadlocks,
    DEADLOCK_CHECK_SECONDS,
    LockedResource,
    LockHolder,
} from "./transaction/Lock";
import { Connection, ConnectionManager } from "./common/connection/connection";
import { pretty_print } from "cc.pretty";

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

class TxManager {
    private transactionsByUuid = new LuaMap<string, TxWorker>();

    private transactionsByHolder = new LuaMap<LockHolder, TxWorker>();

    private transactionsByConnection = new LuaMap<Connection, LuaSet<TxWorker>>();

    private connectionManager = new ConnectionManager("kv2");

    private db: DirKvStore;

    public constructor(db: DirKvStore) {
        this.db = db;
    }

    private newTransaction(conn: Connection): TxWorker {
        const uuid = uuid4();
        const tx = this.db.begin();
        const txw = new TxWorker(conn, tx, uuid);
        this.transactionsByUuid.set(uuid, txw);
        this.transactionsByHolder.set(txw.transaction.holder, txw);

        if (!this.transactionsByConnection.has(conn)) {
            // The thread calling the closure is the same as the one handling all other
            // transaction work, so there are no rollback races.
            conn.onClose(() => this.handleConnectionClosed(conn));
            this.transactionsByConnection.set(conn, new LuaSet());
        }

        assert(this.transactionsByConnection.get(conn)).add(txw);
        return txw;
    }

    private deleteTransaction(txw: TxWorker) {
        this.transactionsByUuid.delete(txw.uuid);
        this.transactionsByHolder.delete(txw.transaction.holder);
        assert(this.transactionsByConnection.get(txw.connection)).delete(txw);
    }

    private getTransaction(txId: string, conn: Connection): TxWorker | undefined {
        const out = this.transactionsByUuid.get(txId);
        if (out && out.connection.connectionId == conn.connectionId) { return out; }
    }

    private handleReleasedLock(resource: LockedResource): void {
        for (const holder of resource.holdersToNotify()) {
            const txw = assert(this.transactionsByHolder.get(holder));
            const wy = txw.resume({ ty: "resume_lock" });
            this.handleWorkerYield(txw, wy);
        }
    }

    private handleWorkerYield(txw: TxWorker, wy: WorkerYield): void {
        if (wy.ty == "done_aborted") {
            this.deleteTransaction(txw);
            for (const res of wy.releasedLocks) { this.handleReleasedLock(res); }
        } else if (wy.ty == "done_err") {
            this.deleteTransaction(txw);
            throw wy.message;
        } else if (wy.ty == "yield_lock") {
            // Pass
        } else if (wy.ty == "done_ok") {
            this.deleteTransaction(txw);
            for (const res of wy.releasedLocks) { this.handleReleasedLock(res); }
        } else if (wy.ty == "yield_operation") {
            // Pass
        } else {
            wy satisfies never;
        }
    }

    public abortTransaction(txw: TxWorker, message: string) {
        const wy = txw.resume(<WorkerResume>{ ty: "resume_abort", message });
        this.handleWorkerYield(txw, wy);
    }

    private getTxwOrSendError(
        txId: string,
        conn: Connection,
        request: Request,
    ): TxWorker | undefined {
        const txw = this.getTransaction(txId, conn);
        if (txw) { return txw; }
        conn.send(<Response>{
            ty: "kv2apiv1response",
            id: request.id,
            result: {
                ok: false,
                message: "transaction not found",
            },
        });
    }

    private enqueueRequest(txw: TxWorker, req: Request) {
        txw.reqQueue.pushBack(req);
        if (txw.lastYield.ty == "yield_operation") {
            this.handleWorkerYield(txw, txw.resume({ ty: "resume_operation" }));
        }
    }

    private handleConnectionClosed(conn: Connection) {
        for (const txw of assert(this.transactionsByConnection.get(conn))) {
            // abortTransaction is ok to call since a closed conn drops all replies
            // instead of raising an error.
            this.abortTransaction(txw, "connection closed");
        }
        this.transactionsByConnection.delete(conn);
    }

    public handleRequest(conn: Connection, req: Request) {
        const op = req.op;
        const ty = op.ty;
        if (ty == "begin") {
            const txw = this.newTransaction(conn);
            this.enqueueRequest(txw, req);
        } else if (ty == "get" || ty == "find" || ty == "set") {
            print("find");
            if (op.txId) {
                const txw = this.getTxwOrSendError(op.txId, conn, req);
                if (!txw) { return; }
                this.enqueueRequest(txw, req);
            } else {
                print("nt");
                const txw = this.newTransaction(conn);
                this.enqueueRequest(txw, req);
            }
        } else if (ty == "commit" || ty == "rollback") {
            const txw = this.getTxwOrSendError(op.txId, conn, req);
            if (!txw) { return; }
            this.enqueueRequest(txw, req);
        } else {
            ty satisfies never;
        }
    }

    public mainLoop() {
        const requestHandler = (): never => {
            while (true) {
                const bkTimer = this.connectionManager.doBookkeeping();
                const [sender, msg] = rednet.receive("kv2", bkTimer);
                pretty_print(msg);
                if (sender) {
                    const transportMsg = this.connectionManager.handleRednetMessage(
                        sender,
                        msg,
                        "kv2",
                    );

                    if (transportMsg) {
                        if (isRequest(transportMsg.payload)) {
                            this.handleRequest(
                                transportMsg.connection,
                                transportMsg.payload,
                            );
                        } else {
                            transportMsg.connection.close();
                        }
                    }
                }
            }
        };

        const deadlockBreaker = (): never => {
            while (true) {
                sleep(DEADLOCK_CHECK_SECONDS);
                for (const holder of breakDeadlocks()) {
                    const txw = assert(this.transactionsByHolder.get(holder));
                    assert(txw.lastYield.ty == "yield_lock");
                    this.abortTransaction(txw, "deadlock detected");
                }
            }
        };

        parallel.waitForAll(requestHandler, deadlockBreaker);
    }
}
