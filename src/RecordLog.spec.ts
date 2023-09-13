import { RecordLog } from "./RecordLog";
import { Namespace, PageNum, PageSize } from "./store/IPageStore";
import { MemCollection } from "./store/MemStore";

{
    const mem = new MemCollection(16 as PageSize).getStore(0 as Namespace);

    // A record log is an append-only disk-backed data structure for string
    // records. Records are indexed by a monotonically increasing Log Sequence
    // Number (LSN).
    const log = new RecordLog(mem);
    const lsn = log.appendRecord("Hello, log!");

    // The last page of the log may stay in memory for performance. We can force
    // it into the store by flushing the LSN.
    log.flushToPoint(lsn);

    // To make very long records possible, records are split across one or more
    // entries in one or more pages, which are then logically coalesced. As a
    // quirk, the very first page of the log has an inacessible 0-length entry
    // at its start.
    // Each entry is stored preceded by its length in little-endian.
    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I1", 0), // 0-sized entry.
        string.pack("<I1", 11), // The length of "Hello, log!"
        "Hello, log!",
    ]));

    // We can retrieve the record.
    assert(log.getRecord(lsn)[0] == "Hello, log!");

    // We need to close the log when we finish using it.
    log.close();
}

{
    // The length field grows as the page size grows. For simplicity, this
    // happens at the powers of 256, although this isn't optimal.

    // 255-byte pages:
    let mem = new MemCollection(255 as PageSize).getStore(0 as Namespace);
    let log = new RecordLog(mem);
    log.flushToPoint(log.appendRecord("foo"));
    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I1", 0), // 1 Byte
        string.pack("<I1", 3),
        "foo",
    ]));
    log.close();

    // 256-byte pages:
    mem = new MemCollection(256 as PageSize).getStore(0 as Namespace);
    log = new RecordLog(mem);
    log.flushToPoint(log.appendRecord("foo"));
    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I2", 0), // 2 Bytes
        string.pack("<I2", 3),
        "foo",
    ]));
    log.close();

    // 65535-byte pages:
    mem = new MemCollection(65535 as PageSize).getStore(0 as Namespace);
    log = new RecordLog(mem);
    log.flushToPoint(log.appendRecord("foo"));
    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I2", 0), // 2 Bytes
        string.pack("<I2", 3),
        "foo",
    ]));
    log.close();

    // 65536-byte pages:
    mem = new MemCollection(65536 as PageSize).getStore(0 as Namespace);
    log = new RecordLog(mem);
    log.flushToPoint(log.appendRecord("foo"));
    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I3", 0), // 3 Bytes
        string.pack("<I3", 3),
        "foo",
    ]));
    log.close();
}

{
    // Records are split in the most trivial way: start a new entry in the next
    // page. The last entry in a full page will always connect to the first
    // entry of the next page to form a record.
    const mem = new MemCollection(16 as PageSize).getStore(0 as Namespace);
    const log = new RecordLog(mem);
    log.flushToPoint(log.appendRecord("0123456789abcdef0123456789abcdef"));

    // The record starts on page 0...
    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I1", 0),
        string.pack("<I1", 14),
        "0123456789abcd",
    ]));

    // ...goes through page 1...
    assert(mem.getPage(1 as PageNum).read() == table.concat([
        string.pack("<I1", 15),
        "ef0123456789abc",
    ]));

    // ...and ends on page 2.
    assert(mem.getPage(2 as PageNum).read() == table.concat([
        string.pack("<I1", 3),
        "def",
    ]));

    log.close();
}

{
    // Records at the end of a full page *always* continue in the next one.
    // If a record just fits in the page, but leaves the page full, it continues
    // as a 0-length entry in the next page.
    const mem = new MemCollection(16 as PageSize).getStore(0 as Namespace);
    const log = new RecordLog(mem);
    log.flushToPoint(log.appendRecord("0123456789abcd"));

    // The record starts on page 0...
    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I1", 0),
        string.pack("<I1", 14),
        "0123456789abcd",
    ]));

    // ...and ends on page 1 as a 0-length entry.
    assert(mem.getPage(1 as PageNum).read() == table.concat([
        string.pack("<I1", 0),
    ]));

    log.close();
}

{
    // A record can also start as a 0-length entry at the end of a page, as its
    // length bytes will fill an otherwise non-full page.
    const mem = new MemCollection(16 as PageSize).getStore(0 as Namespace);
    const log = new RecordLog(mem);
    log.appendRecord("0123456789abc");
    log.flushToPoint(log.appendRecord("hi"));

    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I1", 0),
        string.pack("<I1", 13), // First record.
        "0123456789abc",
        string.pack("<I1", 0), // Part of the second record.
    ]));

    assert(mem.getPage(1 as PageNum).read() == table.concat([
        "\x02", // The "rest" of the second record.
        "hi",
    ]));

    log.close();
}

{
    // If the length field is larger than 1 byte, then the page can be full even
    // if its size is less than the page size.
    const mem = new MemCollection(256 as PageSize).getStore(0 as Namespace);
    const log = new RecordLog(mem);
    log.appendRecord(string.rep("a", 251));
    log.flushToPoint(log.appendRecord("hi"));

    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I2", 0),
        string.pack("<I2", 251),
        string.rep("a", 251),
        // The whole page is 255 bytes long, 1 less than the page size, but we
        // can't fit another entry here so it's considered full.
    ]));

    assert(mem.getPage(1 as PageNum).read() == table.concat([
        string.pack("<I2", 0), // 0-length entry from the first record.
        string.pack("<I2", 2),
        "hi",
    ]));

    log.close();
}

{
    // The LSN for a given record is the page address from the first entry it
    // appears in.
    const mem = new MemCollection(16 as PageSize).getStore(0 as Namespace);
    const log = new RecordLog(mem);
    const lsn1 = log.appendRecord("The LSN for a given");
    const lsn2 = log.appendRecord("record is the page");
    const lsn3 = log.appendRecord("address from the first");
    const lsn4 = log.appendRecord("entry it appears in.");
    log.flushToPoint(lsn4);

    assert(mem.getPage(0 as PageNum).read() == table.concat([
        /* 0 */ string.pack("<I1", 0),
        /* 1 */ string.pack("<I1", 14), // Start of record at lsn1
        /* 2 */ "The LSN for a ",
    ]));

    // getRecord(lsn) returns the whole record and the next LSN.
    assert(lsn1 == 1);
    assert(log.getRecord(lsn1)[0] == "The LSN for a given");
    assert(log.getRecord(lsn1)[1] == lsn2);

    assert(mem.getPage(1 as PageNum).read() == table.concat([
        /* 16 */ string.pack("<I1", 5),
        /* 17 */ "given",
        /* 22 */ string.pack("<I1", 9),  // Start of record at lsn2
        /* 23 */ "record is",
    ]));

    assert(lsn2 == 22);
    assert(log.getRecord(lsn2)[0] == "record is the page");
    assert(log.getRecord(lsn2)[1] == lsn3);

    assert(mem.getPage(2 as PageNum).read() == table.concat([
        /* 32 */ string.pack("<I1", 9),
        /* 33 */ " the page",
        /* 42 */ string.pack("<I1", 5),  // Start of record at lsn3
        /* 43 */ "addre",
    ]));

    assert(lsn3 == 42);
    assert(log.getRecord(lsn3)[0] == "address from the first");
    assert(log.getRecord(lsn3)[1] == lsn4);

    assert(mem.getPage(3 as PageNum).read() == table.concat([
        /* 48 */ string.pack("<I1", 15),
        /* 49 */ "ss from the fir",
    ]));

    assert(mem.getPage(4 as PageNum).read() == table.concat([
        /* 64 */ string.pack("<I1", 2),
        /* 65 */ "st",
        /* 67 */ string.pack("<I1", 12),  // Start of record at lsn4
        /* 68 */ "entry it app",
    ]));

    // For the final record, the next record doesn't exist.
    assert(lsn4 == 67);
    assert(log.getRecord(lsn4)[0] == "entry it appears in.");
    assert(log.getRecord(lsn4)[1] == log.getEnd());

    assert(mem.getPage(5 as PageNum).read() == table.concat([
        /* 80 */ string.pack("<I1", 8),
        /* 81 */ "ears in.",
    ]));

    log.close();
}

{
    // The log can recover from half-written entries in the last page.
    const mem = new MemCollection(16 as PageSize).getStore(0 as Namespace);

    mem.getPage(0 as PageNum).create(table.concat([
        string.pack("<I1", 0),
        string.pack("<I1", 6),
        "hello!",
        string.pack("<I1", 6),
        "wai-", // Should be 6 characters but got cut off.
    ]));

    // getStart() returns the LSN of the first record still stored.
    const log = new RecordLog(mem);
    const lsn1 = log.getStart();
    assert(log.getRecord(lsn1)[0] == "hello!");

    // The "wai-" entry got stripped away.
    assert(log.getRecord(lsn1)[1] == log.getEnd());

    // The page also gets rewritten to fix the torn entry, even without a flush.
    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I1", 0),
        string.pack("<I1", 6),
        "hello!",
    ]));

    log.close();
}

{
    // The log can recover from half-written records in the last page.
    const mem = new MemCollection(16 as PageSize).getStore(0 as Namespace);

    mem.getPage(0 as PageNum).create(table.concat([
        string.pack("<I1", 0),
        string.pack("<I1", 14),
        "this is a very",
    ]));

    mem.getPage(1 as PageNum).create(table.concat([
        string.pack("<I1", 11),
        "long rec-",
    ]));

    const log = new RecordLog(mem);

    assert(log.getStart() == log.getEnd());

    // The first page is rectified.
    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I1", 0),
    ]));

    // The second page is deleted.
    assert(mem.getPage(1 as PageNum).read() == undefined);

    log.close();
}

{
    // The log can still recover from half-written records even if there are no
    // half-written entries.
    const mem = new MemCollection(16 as PageSize).getStore(0 as Namespace);

    // The page is full but there's no futher page, so the record is incomplete.
    mem.getPage(0 as PageNum).create(table.concat([
        string.pack("<I1", 0),
        string.pack("<I1", 14),
        "this is a very",
    ]));

    const log = new RecordLog(mem);

    assert(log.getStart() == log.getEnd());

    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I1", 0),
    ]));

    log.close();
}
