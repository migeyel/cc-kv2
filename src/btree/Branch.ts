import { PAGE_LINK_BYTES } from "../records/SizeClass";
import { VarRecordId, VarRecordsComponent } from "../records/VarRecords";
import { PageNum } from "../store/IPageStore";
import { PAGE_FMT } from "../txStore/LogRecord/types";
import { IEvent, IObj } from "../txStore/LogStore";
import { uIntLenBytes } from "../util";

/** The maximum amount of entries that can fit in a branch node. */
export const MAX_BRANCH_ENTRY = 65534;

/** How many bytes it takes to store an index into a leaf entry. */
const ENTRY_IDX_BYTES = uIntLenBytes(MAX_BRANCH_ENTRY);
const ENTRY_LEN_BYTES = uIntLenBytes(MAX_BRANCH_ENTRY + 1);
const ENTRY_IDX_FMT = "I" + ENTRY_IDX_BYTES;
const ENTRY_LEN_FMT = "I" + ENTRY_LEN_BYTES;

const INIT_BRANCH_FMT = "<BB" + PAGE_FMT;

enum BranchEventType {
    INIT,
    DEINIT,
    ADD_RKEY,
    ADD_LKEY,
    DEL_RKEY,
    DEL_LKEY,
    SET_KEY,
}

export enum WithChild {
    LEFT,
    RIGHT,
}

/** Initializes a node, adding in its first value with no keys. */
export class InitBranchEvent implements IEvent {
    public ty = BranchEventType.INIT as const;
    public height: number;
    public child: PageNum;

    public constructor(height: number, child: PageNum) {
        this.height = height;
        this.child = child;
    }

    public serialize(): string {
        return string.pack(
            INIT_BRANCH_FMT,
            this.ty,
            this.height,
            this.child,
        );
    }

    public static deserialize(str: string): InitBranchEvent {
        const [_, height, value] = string.unpack(INIT_BRANCH_FMT, str);
        return new InitBranchEvent(height, value);
    }
}

/** Deinitializes a node, removing its sole value. */
export class DeinitBranchEvent implements IEvent {
    public ty = BranchEventType.DEINIT as const;

    public serialize(): string {
        return string.char(this.ty);
    }
}

const ADD_BRANCH_FMT = "<B" + ENTRY_IDX_FMT + PAGE_FMT;

/** Adds a key entry and a child. */
export class AddBranchKeyEvent implements IEvent {
    public ty: BranchEventType.ADD_RKEY | BranchEventType.ADD_LKEY;
    public pos: number;
    public child: PageNum;
    public key: VarRecordId;

    public constructor(
        pos: number,
        child: PageNum,
        key: VarRecordId,
        withChild: WithChild,
    ) {
        this.ty = withChild == WithChild.RIGHT ?
            BranchEventType.ADD_RKEY :
            BranchEventType.ADD_LKEY;
        this.pos = pos;
        this.child = child;
        this.key = key;
    }

    public serialize(): string {
        return string.pack(ADD_BRANCH_FMT, this.ty, this.pos, this.child) +
            this.key.serialize();
    }

    public static deserialize(str: string): AddBranchKeyEvent {
        const [ty, pos, value, a1] = string.unpack(ADD_BRANCH_FMT, str);
        const [key] = VarRecordId.deserialize(str, a1);
        const withChild = ty == BranchEventType.ADD_RKEY ?
            WithChild.RIGHT :
            WithChild.LEFT;
        return new AddBranchKeyEvent(pos, value, key, withChild);
    }
}

const DEL_BRANCH_FMT = "<B" + ENTRY_IDX_FMT;

/** Removes a key entry and a child. */
export class DelBranchKeyEvent implements IEvent {
    public ty: BranchEventType.DEL_RKEY | BranchEventType.DEL_LKEY;
    public pos: number;

    public constructor(pos: number, withChild: WithChild) {
        this.ty = withChild == WithChild.RIGHT ?
            BranchEventType.DEL_RKEY :
            BranchEventType.DEL_LKEY;
        this.pos = pos;
    }

    public serialize(): string {
        return string.pack(DEL_BRANCH_FMT, this.ty, this.pos);
    }

    public static deserialize(str: string): DelBranchKeyEvent {
        const [ty, pos] = string.unpack(DEL_BRANCH_FMT, str);
        const withChild = ty == BranchEventType.DEL_RKEY ?
            WithChild.RIGHT :
            WithChild.LEFT;
        return new DelBranchKeyEvent(pos, withChild);
    }
}

const SET_BRANCH_FMT = "<B" + ENTRY_IDX_FMT;

/** Modifies a key entry. */
export class SetBranchKeyEvent implements IEvent {
    public ty = BranchEventType.SET_KEY as const;
    public pos: number;
    public key: VarRecordId;

    public constructor(pos: number, key: VarRecordId) {
        this.pos = pos;
        this.key = key;
    }

    public serialize(): string {
        return string.pack(SET_BRANCH_FMT, this.ty, this.pos) +
            this.key.serialize();
    }

    public static deserialize(str: string): SetBranchKeyEvent {
        const [_, pos, a1] = string.unpack(SET_BRANCH_FMT, str);
        const [key] = VarRecordId.deserialize(str, a1);
        return new SetBranchKeyEvent(pos, key);
    }
}

export type BranchEvent =
    | InitBranchEvent
    | DeinitBranchEvent
    | AddBranchKeyEvent
    | DelBranchKeyEvent
    | SetBranchKeyEvent;

const BRANCH_FMT = "<B" + ENTRY_LEN_FMT + ENTRY_LEN_FMT;

/** The byte overhead for a branch page with no (additional) entries. */
export const BRANCH_OVERHEAD = 1 + 2 * ENTRY_IDX_BYTES + PAGE_LINK_BYTES;

/** A B+ tree branch node with sorted keys. */
export class BranchObj implements IObj<BranchEvent> {
    public readonly type = "branch";
    public height: number;
    public children: PageNum[];
    public keys: VarRecordId[];
    public usedSpace: number;

    public constructor(height: number, vals: PageNum[], keys: VarRecordId[]) {
        this.height = height;
        this.children = vals;
        this.keys = keys;
        this.usedSpace = vals.length * PAGE_LINK_BYTES;
        for (const k of keys) { this.usedSpace += k.length(); }
    }

    /** Returns the maximum byte size a leaf entry can take in the node. */
    public static getMaxEntrySize(vrc: VarRecordsComponent) {
        return vrc.maxVidLen + PAGE_LINK_BYTES;
    }

    /** Returns a reasonable index to split this node in half. */
    public getSplitIndex(): number {
        let sum = 0;
        let index = 0;
        while (sum < this.usedSpace / 2) {
            sum += this.keys[index].length() + PAGE_LINK_BYTES;
            index++;
        }
        return index;
    }

    public apply(event: BranchEvent): void {
        if (event.ty == BranchEventType.ADD_LKEY) {
            this.children.splice(event.pos, 0, event.child);
            this.keys.splice(event.pos, 0, event.key);
            this.usedSpace += PAGE_LINK_BYTES + event.key.length();
        } else if (event.ty == BranchEventType.ADD_RKEY) {
            this.children.splice(event.pos + 1, 0, event.child);
            this.keys.splice(event.pos, 0, event.key);
            this.usedSpace += PAGE_LINK_BYTES + event.key.length();
        } else if (event.ty == BranchEventType.DEL_LKEY) {
            this.children.splice(event.pos, 1);
            this.usedSpace -= PAGE_LINK_BYTES;
            this.usedSpace -= this.keys.splice(event.pos, 1)[0].length();
        } else if (event.ty == BranchEventType.DEL_RKEY) {
            this.children.splice(event.pos + 1, 1);
            this.usedSpace -= PAGE_LINK_BYTES;
            this.usedSpace -= this.keys.splice(event.pos, 1)[0].length();
        } else if (event.ty == BranchEventType.INIT) {
            assert(this.isEmpty(), "can't init a node twice");
            this.height = event.height;
            this.children[0] = event.child;
        } else if (event.ty == BranchEventType.SET_KEY) {
            assert(this.keys[event.pos], "can't set an out-of-range key");
            this.keys[event.pos] = event.key;
        } else {
            const isUnary = this.children.length == 1 && this.keys.length == 0;
            assert(isUnary, "can't deinit a node with more than 1 value");
            this.children.pop();
        }
    }

    public isEmpty(): boolean {
        return this.children.length == 0 && this.keys.length == 0;
    }

    public serialize(): string {
        const out = [string.pack(
            BRANCH_FMT + string.rep(PAGE_FMT, this.children.length),
            this.height,
            this.children.length,
            this.keys.length,
            ...this.children,
        )];
        for (const k of this.keys) { out.push(k.serialize()); }
        return table.concat(out);
    }
}

export function deserializeBranchObj(str?: string): BranchObj {
    if (!str) { return new BranchObj(255, [], []); }

    const [height, valsLength, keysLength, a1] = string.unpack(BRANCH_FMT, str);
    const vals = string.unpack("<" + string.rep(PAGE_FMT, valsLength), str, a1);
    let pos = vals.pop();

    const keys = [];
    for (const _ of $range(1, keysLength)) {
        const [key, nextPos] = VarRecordId.deserialize(str, pos);
        keys.push(key);
        pos = nextPos;
    }

    return new BranchObj(height, vals, keys);
}

export function deserializeBranchEvent(str: string): BranchEvent {
    const ty = string.byte(str);
    if (ty == BranchEventType.ADD_LKEY || ty == BranchEventType.ADD_RKEY) {
        return AddBranchKeyEvent.deserialize(str);
    } else if (
        ty == BranchEventType.DEL_LKEY || ty == BranchEventType.DEL_RKEY
    ) {
        return DelBranchKeyEvent.deserialize(str);
    } else if (ty == BranchEventType.INIT) {
        return InitBranchEvent.deserialize(str);
    } else if (ty == BranchEventType.DEINIT) {
        return new DeinitBranchEvent();
    } else if (ty == BranchEventType.SET_KEY) {
        return SetBranchKeyEvent.deserialize(str);
    } else {
        throw new Error("Unknown branch event type: " + ty);
    }
}
