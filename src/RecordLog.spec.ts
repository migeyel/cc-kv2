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
    // Each entry is stored preceded by its length as 2 little-endian bytes.
    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I2", 0), // 0-sized entry.
        string.pack("<I2", 11), // The length of "Hello, log!"
        "Hello, log!",
    ]));

    // We can retrieve the record.
    assert(log.getRecord(lsn)[0] == "Hello, log!");

    // We need to close the log when we finish using it.
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
        string.pack("<I2", 0),
        string.pack("<I2", 12),
        "0123456789ab",
    ]));

    // ...goes through page 1...
    assert(mem.getPage(1 as PageNum).read() == table.concat([
        string.pack("<I2", 14),
        "cdef0123456789",
    ]));

    // ...and ends on page 2.
    assert(mem.getPage(2 as PageNum).read() == table.concat([
        string.pack("<I2", 6),
        "abcdef",
    ]));

    log.close();
}

{
    // Records at the end of a full page *always* continue in the next one.
    // If a record just fits in the page, but leaves the page full, it continues
    // as a 0-length entry in the next page.
    const mem = new MemCollection(16 as PageSize).getStore(0 as Namespace);
    const log = new RecordLog(mem);
    log.flushToPoint(log.appendRecord("0123456789ab"));

    // The record starts on page 0...
    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I2", 0),
        string.pack("<I2", 12),
        "0123456789ab",
    ]));

    // ...and ends on page 1 as a 0-length entry.
    assert(mem.getPage(1 as PageNum).read() == table.concat([
        string.pack("<I2", 0),
    ]));

    log.close();
}

{
    // A record can also start as a 0-length entry at the end of a page, as its
    // length bytes will fill an otherwise non-full page.
    const mem = new MemCollection(16 as PageSize).getStore(0 as Namespace);
    const log = new RecordLog(mem);
    const lsn1 = log.appendRecord("0123456789");
    log.flushToPoint(log.appendRecord("hi"));

    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I2", 0),
        string.pack("<I2", 10), // First record.
        "0123456789",
        string.pack("<I2", 0), // Part of the second record.
    ]));

    assert(mem.getPage(1 as PageNum).read() == table.concat([
        string.pack("<I2", 2), // The "rest" of the second record.
        "hi",
    ]));

    assert(log.getRecord(lsn1)[0] == "0123456789");

    log.close();
}

{
    // The page can be full even if its size is less than the page size.
    const mem = new MemCollection(16 as PageSize).getStore(0 as Namespace);
    const log = new RecordLog(mem);
    log.appendRecord("0123456789a");
    log.flushToPoint(log.appendRecord("hi"));

    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I2", 0),
        string.pack("<I2", 11),
        "0123456789a",
        // The whole page is 15 bytes long, 1 less than the page size, but we
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
        /* 0 */ string.pack("<I2", 0),
        /* 2 */ string.pack("<I2", 12), // Start of record at lsn1
        /* 4 */ "The LSN for ",
    ]));

    // getRecord(lsn) returns the whole record and the next LSN.
    assert(lsn1 == 2);
    assert(log.getRecord(lsn1)[0] == "The LSN for a given");
    assert(log.getRecord(lsn1)[1] == lsn2);

    assert(mem.getPage(1 as PageNum).read() == table.concat([
        /* 16 */ string.pack("<I2", 7),
        /* 18 */ "a given",
        /* 25 */ string.pack("<I2", 5),  // Start of record at lsn2
        /* 27 */ "recor",
    ]));

    assert(lsn2 == 25);
    assert(log.getRecord(lsn2)[0] == "record is the page");
    assert(log.getRecord(lsn2)[1] == lsn3);

    assert(mem.getPage(2 as PageNum).read() == table.concat([
        /* 32 */ string.pack("<I2", 13),
        /* 34 */ "d is the page",
    ]));

    assert(mem.getPage(3 as PageNum).read() == table.concat([
        /* 48 */ string.pack("<I2", 0),
        /* 50 */ string.pack("<I2", 12), // Start of record at lsn3
        /* 52 */ "address from",
    ]));

    assert(lsn3 == 50);
    assert(log.getRecord(lsn3)[0] == "address from the first");
    assert(log.getRecord(lsn3)[1] == lsn4);

    assert(mem.getPage(4 as PageNum).read() == table.concat([
        /* 64 */ string.pack("<I2", 10),
        /* 66 */ " the first",
        /* 76 */ string.pack("<I2", 2),  // Start of record at lsn4
        /* 78 */ "en",
    ]));

    // For the final record, the next record doesn't exist.
    assert(lsn4 == 76);
    assert(log.getRecord(lsn4)[0] == "entry it appears in.");
    assert(log.getRecord(lsn4)[1] == log.getEnd());

    assert(mem.getPage(5 as PageNum).read() == table.concat([
        /* 80 */ string.pack("<I2", 14),
        /* 82 */ "try it appears",
    ]));

    assert(mem.getPage(6 as PageNum).read() == table.concat([
        /* 96 */ string.pack("<I2", 4),
        /* 98 */ " in.",
    ]));

    log.close();
}

{
    // The log can recover from half-written entries in the last page.
    const mem = new MemCollection(32 as PageSize).getStore(0 as Namespace);

    mem.getPage(0 as PageNum).create(table.concat([
        string.pack("<I2", 0),
        string.pack("<I2", 6),
        "hello!",
        string.pack("<I2", 6),
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
        string.pack("<I2", 0),
        string.pack("<I2", 6),
        "hello!",
    ]));

    log.close();
}

{
    // The log can recover from half-written records in the last page.
    const mem = new MemCollection(16 as PageSize).getStore(0 as Namespace);

    mem.getPage(0 as PageNum).create(table.concat([
        string.pack("<I2", 0),
        string.pack("<I2", 12),
        "this is a ve",
    ]));

    mem.getPage(1 as PageNum).create(table.concat([
        string.pack("<I2", 14),
        "ry long rec-",
    ]));

    const log = new RecordLog(mem);

    assert(log.getStart() == log.getEnd());

    // The first page is rectified.
    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I2", 0),
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
        string.pack("<I2", 0),
        string.pack("<I2", 12),
        "this is a ve",
    ]));

    const log = new RecordLog(mem);

    assert(log.getStart() == log.getEnd());

    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I2", 0),
    ]));

    log.close();
}

{
    // The log can be trimmed to remove old records that aren't needed.
    const mem = new MemCollection(16 as PageSize).getStore(0 as Namespace);
    const log = new RecordLog(mem);

    const lsn1 = log.appendRecord("0123456789abcdef0123456789abcdef");
    const lsn2 = log.appendRecord("abcd");
    log.flushToPoint(lsn2);

    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I2", 0),
        string.pack("<I2", 12),
        "0123456789ab",
    ]));

    assert(mem.getPage(1 as PageNum).read() == table.concat([
        string.pack("<I2", 14),
        "cdef0123456789",
    ]));

    assert(mem.getPage(2 as PageNum).read() == table.concat([
        string.pack("<I2", 6),
        "abcdef",
        string.pack("<I2", 4),
        "abcd",
    ]));

    // Trimming to lsn1 doesn't free any page.
    log.trimToPoint(lsn1);
    assert(mem.getPage(0 as PageNum).read() == table.concat([
        string.pack("<I2", 0),
        string.pack("<I2", 12),
        "0123456789ab",
    ]));

    // Trimming to lsn2 frees the first 2 pages.
    log.trimToPoint(lsn2);
    assert(mem.getPage(0 as PageNum).read() == undefined);
    assert(mem.getPage(1 as PageNum).read() == undefined);
    assert(mem.getPage(2 as PageNum).read() == table.concat([
        string.pack("<I2", 6),
        "abcdef",
        string.pack("<I2", 4),
        "abcd",
    ]));
}
