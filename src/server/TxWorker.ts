import { Request, Response, ResponseError, ResponseOp } from "../common/apis/userApi";
import { Connection } from "../common/connection/connection";
import { Deque } from "../common/Deque";
import { LOCK_RELEASED_EVENT } from "../core/transaction/Lock";
import { Transaction } from "../core/transaction/Transaction";
import {
    DoneOk,
    WorkerDone,
    WorkerResume,
    WorkerYield,
} from "./txWorkerApi";

export class TxWorker {
    /** The worker's Lua thread. */
    public thread: LuaThread;

    /** The worker's transaction. */
    public transaction: Transaction;

    /** External transaction UUID. */
    public uuid: string;

    /** The connection this transaction is working through. */
    public connection: Connection;

    /** The queue of incoming requests that this worker will handle. */
    public reqQueue = new Deque<Request>();

    /** The last value the worker yielded with. */
    public lastYield: WorkerYield;

    private sendOk(req: Request, op: ResponseOp) {
        this.connection.send(<Response>{
            ty: "kv2apiv1response",
            id: req.id,
            result: {
                ok: true,
                op,
            },
        });
    }

    private workerLoop(): DoneOk {
        // Handle the first request. Can either be a begin statement or an
        // auto-commit single operation.
        {
            while (this.reqQueue.isEmpty()) {
                coroutine.yield({ ty: "yield_operation" });
            }

            // We let the request stay on the queue while we process it so it can be
            // used for sending a response after an abort.
            const reqNode = assert(this.reqQueue.first());
            const req = reqNode.val;
            const op = req.op;
            if (op.ty == "begin") {
                this.sendOk(req, { ty: "begin", txId: this.uuid });
            } else if (op.ty == "get") {
                const value = this.transaction.get(op.key);
                const releasedLocks = this.transaction.commit();
                this.sendOk(req, { ty: "get", value });
                reqNode.pop();
                return { ty: "done_ok", releasedLocks };
            } else if (op.ty == "set") {
                if (op.value) {
                    this.transaction.set(op.key, op.value);
                } else {
                    this.transaction.delete(op.key);
                }
                const releasedLocks = this.transaction.commit();
                this.sendOk(req, { ty: "set" });
                reqNode.pop();
                return { ty: "done_ok", releasedLocks };
            } else if (op.ty == "find") {
                const [eprev, inext] = this.transaction.find(op.key);
                const releasedLocks = this.transaction.commit();
                this.sendOk(req, { ty: "find", eprev, inext });
                reqNode.pop();
                return { ty: "done_ok", releasedLocks };
            } else {
                throw "unexpected first operation type";
            }
            reqNode.pop();
        }

        while (true) {
            while (this.reqQueue.isEmpty()) {
                coroutine.yield({ ty: "yield_operation" });
            }

            // Same reasoning as above.
            const reqNode = assert(this.reqQueue.first());
            const req = reqNode.val;
            const op = req.op;
            if (op.ty == "get") {
                const value = this.transaction.get(op.key);
                this.sendOk(req, { ty: "get", value });
            } else if (op.ty == "set") {
                if (op.value) {
                    this.transaction.set(op.key, op.value);
                } else {
                    this.transaction.delete(op.key);
                }
                this.sendOk(req, { ty: "set" });
            } else if (op.ty == "find") {
                const [eprev, inext] = this.transaction.find(op.key);
                this.sendOk(req, { ty: "find", eprev, inext });
            } else if (op.ty == "commit") {
                const releasedLocks = this.transaction.commit();
                this.sendOk(req, { ty: "commit" });
                reqNode.pop();
                return { ty: "done_ok", releasedLocks };
            } else if (op.ty == "rollback") {
                const releasedLocks = this.transaction.rollback();
                this.sendOk(req, { ty: "rollback" });
                reqNode.pop();
                return { ty: "done_ok", releasedLocks };
            } else if (op.ty == "begin") {
                throw "unreachable";
            } else {
                op satisfies never;
            }
            reqNode.pop();
        }
    }

    /** Replies with an error to all remaining requests on the queue. */
    private errorBulkReply(err: ResponseError) {
        while (true) {
            const req = this.reqQueue.popFront();
            if (!req) { break; }
            this.connection.send(<Response>{
                ty: "kv2apiv1response",
                id: req.id,
                result: err,
            });
        }
    }

    private entrypoint(): WorkerDone {
        const [ok, out] = pcall(() => this.workerLoop());

        if (ok) {
            return out;
        } else {
            this.errorBulkReply({
                ok: false,
                message: "Unexpected error: " + tostring(out),
            });

            return <WorkerDone>{
                ty: "done_err",
                message: tostring(out),
            };
        }
    }

    public resume(resume: WorkerResume): WorkerYield {
        if (resume.ty == "resume_operation" || resume.ty == "resume_lock") {
            // Resume as normal.
            // Lock code doesn't care about the yield's return value, so we can give
            // it the WorkerResume.
            const [ok, yvalue] = coroutine.resume(this.thread, resume);
            assert(ok, yvalue);

            if (type(yvalue) == "table") {
                // Yielded/returned a direct WorkerYield value, which is allowed.
                this.lastYield = yvalue;
            } else if (yvalue == LOCK_RELEASED_EVENT) {
                // Lock code yielded for a LOCK_RELEASED_EVENT.
                this.lastYield = { ty: "yield_lock" };
            } else {
                // Yielded for an unknown event, which we forbid so as to not break db
                // code atomicity invariants.
                const msg = "transaction worker yielded an unexpected value";
                error(debug.traceback(this.thread, msg));
            }
        } else if (resume.ty == "resume_abort") {
            // Cancel running the coroutine and abort.
            // This is safe because we only allow the coroutine to yield on places where
            // it can be cancelled, and where the abort code can run safely.
            this.errorBulkReply({
                ok: false,
                message: "Transaction aborted: " + resume.message,
                aborted: this.uuid,
            });

            this.lastYield = {
                ty: "done_aborted",
                message: resume.message,
                releasedLocks: this.transaction.rollback(),
            };
        } else {
            resume satisfies never;
        }

        return this.lastYield;
    }

    public constructor(
        connection: Connection,
        transaction: Transaction,
        uuid: string,
    ) {
        this.thread = coroutine.create(() => this.entrypoint());
        this.transaction = transaction;
        this.uuid = uuid;
        this.connection = connection;
        const [ok, yvalue] = coroutine.resume(this.thread);
        assert(ok, yvalue);
        this.lastYield = yvalue;
    }
}
