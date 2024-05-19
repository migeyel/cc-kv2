import { LockedResource } from "../core/transaction/Lock";

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
