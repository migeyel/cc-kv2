import { LockedResource } from "./Lock";

/** Signals that the pending lock may have been released. */
export type ResumeLock = {
    ty: "resume_lock",
};

/** A new operation for the transaction to perform. */
export type ResumeOperation = {
    ty: "resume_operation",
}

/** Signals that the transaction is to be aborted. */
export type ResumeAbort = {
    ty: "resume_abort",
    message: string,
};

export type WorkerResume =
    | ResumeLock
    | ResumeOperation
    | ResumeAbort;

/** Signals that a worker is waiting on a lock. */
export type YieldLock = {
    ty: "yield_lock",
};

/** Signals that a worker is waiting for new tasks. */
export type YieldOperation = {
    ty: "yield_operation",
};

/** Signals that a transaction has finished with no errors. */
export type DoneOk = {
    ty: "done_ok",
    releasedLocks: LuaSet<LockedResource>,
}

/** Signals that a transaction has been aborted by a ResumeAbort operation. */
export type DoneAborted = {
    ty: "done_aborted",
    releasedLocks: LuaSet<LockedResource>,
    message: string,
}

/** Signals that a transaction has finished with an unexpected error. */
export type DoneErr = {
    ty: "done_err",
    message: string,
}

export type WorkerDone =
    | DoneOk
    | DoneAborted
    | DoneErr;

export type WorkerYield =
    | YieldLock
    | YieldOperation
    | DoneOk
    | DoneAborted
    | DoneErr;

/** Thrown by a waiting transaction when it encounters a ResumeAbort. */
export class AbortedError extends Error {
    public constructor(message: string) {
        super(message);
    }
}

/**
 * Yields a value to the transaction manager and gets something back.
 * @param val The value to yield.
 * @returns The response.
 * @throws An instance of AbortedError if the response is a ResumeAbort.
 */
export function workerYield(val: WorkerYield): WorkerResume {
    const [out] = coroutine.yield(val);
    assert(type(out) == "table");
    assert(type(out.ty) == "string");
    const wr = out as WorkerResume;
    if (wr.ty == "resume_abort") { throw new AbortedError(wr.message); }
    return wr;
}
