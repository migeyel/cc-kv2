import { PageNum } from "../store/IPageStore";
import { IEvent, IObj } from "../txStore/LogStore";
import { NO_LINK } from "./RecordPage";
import { MAX_SIZE_CLASS, PAGE_LINK_BYTES, SizeClass } from "./SizeClass";

const EVENT_FMT = "<" + string.rep("I" + PAGE_LINK_BYTES, 2);
const LINKS_FMT = "<" + string.rep("I" + PAGE_LINK_BYTES, MAX_SIZE_CLASS + 1);

// TODO Idk when or where to check that this value holds.
/** The smallest page size that can hold the full header. */
export const MIN_PAGE_SIZE = PAGE_LINK_BYTES * (MAX_SIZE_CLASS + 1);

/** A header for several size class linked-lists. Contains an array of links. */
export class HeaderObj implements IObj<HeaderEvent> {
    public links: PageNum[];

    public constructor(links: PageNum[]) {
        this.links = links;
    }

    public apply(event: HeaderEvent): void {
        this.links[event.index] = event.value;
    }

    public isEmpty(): boolean {
        for (const l of this.links) { if (l != NO_LINK) { return false; } }
        return true;
    }

    public serialize(): string {
        return string.pack(LINKS_FMT, ...this.links);
    }
}

export class HeaderEvent implements IEvent {
    public index: SizeClass;
    public value: PageNum;

    public constructor(index: SizeClass, value: PageNum) {
        this.index = index;
        this.value = value;
    }

    public serialize(): string {
        return string.pack(EVENT_FMT, this.index, this.value);
    }
}

export function deserializeHeaderObj(str?: string): HeaderObj {
    if (str) {
        const links = string.unpack(LINKS_FMT, str);
        links.pop();
        return new HeaderObj(links);
    } else {
        const links = [];
        for (const i of $range(0, MAX_SIZE_CLASS)) {
            links[i] = NO_LINK;
        }
        return new HeaderObj(links);
    }
}

export function deserializeHeaderEvent(str: string): HeaderEvent {
    const [index, value] = string.unpack(EVENT_FMT, str);
    return new HeaderEvent(index, value);
}
