/**
 * An unstable API for administrative commands. Not stable.
 * @module
 */

export type StatusRequestOp = {
    ty: "status",
};

/** Server is OK. */
export type OkServerStatus = {
    ty: "ok",
    usedPages: number,
    quotaPages: number,
    numDataDirs: number,
};

/** Server is not OK. */
export type ErrServerStatus = {
    ty: "err"
    message: string,
};

export type ServerStatus = OkServerStatus | ErrServerStatus;

export type StatusResponseOp = {
    ty: "status",
    status: ServerStatus,
};

/** Add a new data directory. */
export type AddDataDirRequestOp = {
    ty: "adddatadir",
    dir: string,
};

export type AddDataDirResponseOp = {
    ty: "adddatadir",
    dir: string,
};

/** Delete a data directory. */
export type DelDataDirRequestOp = {
    ty: "deldatadir",
    dir: string,
};

export type DelDataDirResponseOp = {
    ty: "deldatadir",
    dir: string,
};

/** Modify a data directory. */
export type ModDataDirRequestOp = {
    ty: "moddatadir",
    dir: string,
    quota: number,
};

export type ModDataDirResponseOp = {
    ty: "moddatadir",
    dir: string,
};

/** Gets information on a data dir. */
export type GetDataDirRequestOp = {
    ty: "getdatadir",
    dir: string,
};

export type GetDataDirResponseOp = {
    ty: "moddatadir",
    dir: string,
    used: number,
    quota: number,
};

/** Asks the server to shut down. */
export type ShutdownRequestOp = {
    ty: "shutdown",
};

export type AdminRequest =
    | StatusRequestOp
    | AddDataDirRequestOp
    | DelDataDirRequestOp
    | ModDataDirRequestOp
    | ShutdownRequestOp;

export type AdminResponse =
    | StatusResponseOp
    | AddDataDirResponseOp
    | DelDataDirResponseOp
    | ModDataDirResponseOp
    | GetDataDirResponseOp;
