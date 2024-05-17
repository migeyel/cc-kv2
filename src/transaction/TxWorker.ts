import { Request, Response, ResponseError, ResponseOp } from "../apis/userApi";
import { Connection } from "../connection/connection";
import { Deque } from "../Deque";
import { Transaction } from "./Transaction";
import {
    AbortedError,
    DoneOk,
    WorkerDone,
    WorkerResume,
    workerYield,
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
            while (this.reqQueue.isEmpty()) { workerYield({ ty: "yield_operation" }); }
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
            while (this.reqQueue.isEmpty()) { workerYield({ ty: "yield_operation" }); }
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
            const err = out as any; // pcall types are wrong.
            if (err instanceof AbortedError) {
                // We know that we errored in a workerYield call, which only gets called
                // when it is safe to abort and rollback.
                this.errorBulkReply({
                    ok: false,
                    message: "Transaction aborted: " + err.message,
                    aborted: this.uuid,
                });
                return <WorkerDone>{
                    ty: "done_aborted",
                    releasedLocks: this.transaction.rollback(),
                    message: err.message,
                };
            } else {
                // We don't know anything about how the inner code errored. The state
                // may as well be complete garbage at this point.
                this.errorBulkReply({
                    ok: false,
                    message: "Unexpected error: " + tostring(err),
                });
                return <WorkerDone>{
                    ty: "done_err",
                    message: tostring(err),
                };
            }
        }
    }

    public resume(resume: WorkerResume): WorkerYield {
        const [ok, yvalue] = coroutine.resume(this.thread, resume);
        assert(ok, yvalue);

        this.lastYield = yvalue;

        if (type(yvalue) != "table") {
            error(debug.traceback(this.thread, "tx worker yielded a non-table value"));
        }

        return yvalue;
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
