/**
 * A stable API used by clients to perform data queries on the store.
 * @module
 */

/** Begins a new transaction. */
export type TxBeginRequestOp = {
    ty: "begin",
};

/** Commits a transaction. */
export type TxCommitRequestOp = {
    ty: "commit",
    txId: string,
};

/** Rolls a transaction back. */
export type TxRollbackRequestOp = {
    ty: "rollback",
    txId: string,
};

/** Gets the value at a key. */
export type TxGetRequestOp = {
    ty: "get",
    txId?: string,
    key: string,
};

/** Sets the value at a key. */
export type TxSetRequestOp = {
    ty: "set",
    txId?: string,
    key: string,
    value?: string,
};

/** Gets the next (inclusive) and previous (exclusive) keys and values from a key. */
export type TxFindRequestOp = {
    ty: "find",
    txId?: string,
    key: string,
};

export type RequestOp =
    | TxBeginRequestOp
    | TxCommitRequestOp
    | TxRollbackRequestOp
    | TxGetRequestOp
    | TxSetRequestOp
    | TxFindRequestOp;

/** The top-level structure for all queries. */
export type Request = {
    ty: "kv2apiv1request",

    /** An ID that gets echoed back on the response. */
    id: string,

    op: RequestOp,
};

export function isRequest(t: any): t is Request {
    if (type(t) != "table") { return false; }
    if (t.ty != "kv2apiv1request") { return false; }
    if (type(t.id) != "string") { return false; }
    const op = t.op as RequestOp;
    if (type(op) != "table") { return false; }
    if (op.ty == "begin") {
        return true;
    } else if (op.ty == "commit") {
        if (type(op.txId) != "string") { return false; }
        return true;
    } else if (op.ty == "find") {
        if (type(op.txId) != "string" && op.txId != undefined) { return false; }
        if (type(op.key) != "string") { return false; }
        return true;
    } else if (op.ty == "get") {
        if (type(op.txId) != "string" && op.txId != undefined) { return false; }
        if (type(op.key) != "string") { return false; }
        return true;
    } else if (op.ty == "rollback") {
        if (type(op.txId) != "string") { return false; }
        return true;
    } else if (op.ty == "set") {
        if (type(op.txId) != "string" && op.txId != undefined) { return false; }
        if (type(op.key) != "string") { return false; }
        if (type(op.value) != "string" && op.value != undefined) { return false; }
        return true;
    } else {
        return false;
    }
}

/** The new transaction ID. */
export type TxBeginResponseOp = {
    ty: "begin",
    txId: string,
};

export type TxCommitResponseOp = {
    ty: "commit",
};

export type TxRollbackResponseOp = {
    ty: "rollback",
};

/** The value at a key, if it exists. */
export type TxGetResponseOp = {
    ty: "get",
    value?: string,
};

/** The old value at a key, which has been replaced by the new one, if it exists. */
export type TxSetResponseOp = {
    ty: "set",
};

/** The next (inclusive) and previous (exclusive) values at a key. */
export type TxFindResponseOp = {
    ty: "find",

    inext?: {
        key: string,
        value: string,
    },

    eprev?: {
        key: string,
        value: string,
    },
};

export type ResponseOp =
    | TxBeginResponseOp
    | TxCommitResponseOp
    | TxRollbackResponseOp
    | TxGetResponseOp
    | TxSetResponseOp
    | TxFindResponseOp;

/** A request has been processed and returned its response successfully. */
export type ResponseOk = {
    ok: true,
    op: ResponseOp,
};

/** An error has happened while processing a request. */
export type ResponseError = {
    ok: false,

    /** A human-readable error message. */
    message: string,

    /** If set, the transaction with the given ID has been rolled back. */
    aborted?: string,
};

/** The top-level structure for all responses. */
export type Response = {
    ty: "kv2apiv1response"

    /** The ID sent in the request. */
    id: string,

    result: ResponseOk | ResponseError,
};
