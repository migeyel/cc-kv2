import * as ActRecord from "./ActRecord";
import * as CheckpointRecord from "./CheckpointRecord";
import * as ClrRecord from "./ClrRecord";
import * as CommitRecord from "./CommitRecord";
import { RecordType } from "./types";

export type Record =
    | ActRecord.Record
    | ClrRecord.Record
    | CommitRecord.Record
    | CheckpointRecord.Record;

export function serialize(r: Record): string {
    if (r.ty == RecordType.ACT) {
        return ActRecord.serialize(r);
    } else if (r.ty == RecordType.CHECKPOINT) {
        return CheckpointRecord.serialize(r);
    } else if (r.ty == RecordType.CLR) {
        return ClrRecord.serialize(r);
    } else {
        return CommitRecord.serialize(r);
    }
}

export function getType(str: string): RecordType {
    return string.byte(str);
}

export function deserialize(str: string): Record {
    const ty = string.byte(str);
    if (ty == RecordType.ACT) {
        return ActRecord.deserialize(str);
    } else if (ty == RecordType.CHECKPOINT) {
        return CheckpointRecord.deserialize(str);
    } else if (ty == RecordType.CLR) {
        return ClrRecord.deserialize(str);
    } else if (ty == RecordType.COMMIT) {
        return CommitRecord.deserialize(str);
    } else {
        throw new Error("Unknown type: " + ty);
    }
}
