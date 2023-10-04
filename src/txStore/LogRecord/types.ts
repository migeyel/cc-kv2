import { MAX_NAMESPACE, MAX_PAGE_NUM } from "../../store/IPageStore";
import { uIntLenBytes } from "../../util";

/** The type a log record can have. */
export enum RecordType {
    /** A set of events played against the collection. */
    ACT,

    /** A set of events that undoes a previous act. */
    CLR,

    /** Records that a transaction has committed. */
    COMMIT,

    /** Records the current state of internal data structures. */
    CHECKPOINT,
}

/** Used for tracking when a page was deleted and recreated. */
export enum PageUpdateType {
    /** The page in the event was created through this event. */
    CREATED,

    /** The page in the event wasn't created through this event. */
    OTHER,
}

/** The maximum value for a transaction ID. */
export const MAX_TX_ID = 2 ** 24 - 1;

/** The maximum value for a log sequence number. */
export const MAX_LSN = 2 ** 48 - 1;

/** The maximum size for the dirty page table. */
export const MAX_DPT_SIZE = 2 ** 24 - 1;

/** The packstring for a page number. */
export const PAGE_FMT = "I" + uIntLenBytes(MAX_PAGE_NUM);

/** The packstring for a namespace. */
export const NAMESPACE_FMT = "I" + uIntLenBytes(MAX_NAMESPACE);

/** The packstring for a transaction ID. */
export const TX_ID_FMT = "I" + uIntLenBytes(MAX_TX_ID);

/** The packstring for a LSN. */
export const LSN_FMT = "I" + uIntLenBytes(MAX_LSN);

/** The packstring for the length of the DPT. */
export const DPT_LEN_FMT = "I" + uIntLenBytes(MAX_DPT_SIZE);
