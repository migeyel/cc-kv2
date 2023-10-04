import { Namespace, PageNum } from "../../store/IPageStore";
import { NAMESPACE_FMT, PAGE_FMT, PageUpdateType } from "./types";

const FMT = "<B" + NAMESPACE_FMT + PAGE_FMT;

/** The maximum length an event record can have. */
export const MAX_EVENT_RECORD_LEN = 2 ** 32 - 1;

/** The maximum length a serialized event can have. */
export const MAX_EVENT_LEN = MAX_EVENT_RECORD_LEN - string.packsize(FMT);

export type Record = {
    updateType: PageUpdateType,
    namespace: Namespace,
    pageNum: PageNum,
    event: string,
};

export function serialize(r: Record): string {
    return string.pack(
        FMT,
        r.updateType,
        r.namespace,
        r.pageNum,
    ) + r.event;
}

export function deserialize(str: string): Record {
    const [
        updateType,
        namespace,
        pageNum,
        pos,
    ] = string.unpack(FMT, str);

    return {
        updateType,
        namespace,
        pageNum,
        event: string.sub(str, pos),
    };
}
