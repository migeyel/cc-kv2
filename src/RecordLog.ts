import { IPage, IPageStore } from "./store/IPageStore";

/** An append-only log of string records. */
export class RecordLog {
    private readonly store: IPageStore<IPage>;

    private readonly pageSize: number;

    /** The packstring for an entry. */
    private readonly entryFmt: string;

    /** How many bytes it takes to store the length of a page entry. */
    private readonly entryLengthBytes: number;

    /** The first LSN after the trimmed part of the log. */
    private firstLsn: number;

    /** Number of the first page in the store. */
    private headPageNum: number;

    /** The last page in the store. */
    private tailPage: IPage;

    /** A buffer to eventually append to the tail page. */
    private tailBuf = "";

    /** The total byte size of the tail page and buffer. */
    private tailSize: number;

    /** Returns the LSN of the first entry in the tail page. */
    private tailBaseLsn(): number {
        return this.tailPage.pageNum * this.pageSize;
    }

    /** Gets the LSN of the first record stored in the log. */
    public getStart(): number {
        return this.firstLsn;
    }

    /** Returns the LSN of the next entry to be appended in the log. */
    public getEnd(): number {
        return this.tailBaseLsn() + this.tailSize;
    }

    public isEmpty(): boolean {
        return this.getStart() == this.getEnd();
    }

    public getNumPages(): number {
        return this.tailPage.pageNum - this.headPageNum + 1;
    }

    /** Returns whether no new entries can be added to the tail page. */
    private isTailFull(): boolean {
        return this.tailSize + this.entryLengthBytes > this.pageSize;
    }

    /** Reads a page, taking care of the tail page buffer edge case. */
    private readPage(page: IPage): string | undefined {
        const out = page.read();
        if (!out) { return; }
        if (page.pageNum == this.tailPage.pageNum) {
            return out + this.tailBuf;
        } else {
            return out;
        }
    }

    /** Finishes the current tail page and starts a new one. */
    private turnTailPage() {
        this.tailPage.append(this.tailBuf);
        this.tailPage.closeAppend();
        this.tailPage = this.store.getPage(this.tailPage.pageNum + 1);
        this.tailPage.createOpen();
        this.tailBuf = "";
        this.tailSize = 0;
    }

    /**
     * Flushes the log up to a LSN, including it.
     * @param flushLsn - The point to flush the log to.
     */
    public flushToPoint(flushLsn: number) {
        const diskTailSize = this.tailSize - this.tailBuf.length;
        const tailLastDiskLsn = this.tailBaseLsn() + diskTailSize - 1;
        if (tailLastDiskLsn < flushLsn) {
            // We don't know how big the record is, so just flush everything.
            this.tailPage.append(this.tailBuf);
            this.tailBuf = "";
        }
    }

    /**
     * Marks for deletion records up to a LSN, not including it.
     *
     * There are no guarantees about whether the records will be deleted or not.
     *
     * @param trimLsn - The point to trim the log to.
     */
    public trimToPoint(trimLsn: number) {
        const trimPageNum = math.floor(trimLsn / this.pageSize);
        const trimEnd = math.min(trimPageNum, this.tailPage.pageNum) - 1;
        for (const i of $range(this.headPageNum, trimEnd)) {
            this.store.getPage(i).delete();
        }
        if  (trimEnd + 1 != this.headPageNum) {
            this.headPageNum = trimEnd + 1;
            this.firstLsn = trimLsn;
        }
    }

    /**
     * Reads an entry at a given position.
     * @param lsn - The entry's base LSN.
     * @returns The entry's contents, or nil if the entry is partially written
     * or belongs to a nonexistent page.
     * @returns The LSN of the next entry, or nil if the contents are nil.
     */
    private getEntry(lsn: number): LuaMultiReturn<
        [string, number] | [undefined, undefined]
    > {
        const rem = lsn % this.pageSize;
        const div = (lsn - rem) / this.pageSize;
        const str = this.readPage(this.store.getPage(div));
        if (!str) { return $multi(undefined, undefined); }
        try {
            const [entry, at] = string.unpack(this.entryFmt, str, rem + 1);
            if (at >= this.pageSize - this.entryLengthBytes + 1) {
                // The next entry can't fit here. So it starts in the next page.
                return $multi(entry, (div + 1) * this.pageSize);
            } else {
                return $multi(entry, div * this.pageSize + at - 1);
            }
        } catch (_) {
            return $multi(undefined, undefined);
        }
    }

    /** Appends an entry to the log. Returns the remaining record fragment. */
    private appendEntry(data: string): string {
        const maxWriteSize =
            this.pageSize - this.entryLengthBytes - this.tailSize;
        const writeSize = math.min(data.length, maxWriteSize);
        const entry = string.sub(data, 1, writeSize);
        this.tailBuf += string.pack(this.entryFmt, entry);
        this.tailSize += this.entryLengthBytes + entry.length;
        return string.sub(data, writeSize + 1);
    }

    /** Lists all entries in a page, excluding partially written ones. */
    private listEntries(pageNum: number): string[] {
        let lsn = this.pageSize * pageNum;
        const out: string[] = [];
        do {
            // Read until we hit nil or roll over to the next page.
            const [entry, next] = this.getEntry(lsn);
            if (!entry) { break; }
            out.push(entry);
            lsn = next;
        } while (lsn % this.pageSize != 0);
        return out;
    }

    /**
     * Returns a record at the given LSN.
     *
     * Care must be taken to give the right LSN as input. A wrong input may
     * either throw a failed assertion or return junk.
     *
     * @param lsn - The record's LSN.
     * @returns The record contents.
     * @returns The LSN of the next record.
     */
    public getRecord(lsn: number): LuaMultiReturn<[string, number]> {
        const out: string[] = [];
        do {
            const [entry, nextLsn] = this.getEntry(lsn);
            out.push(assert(entry));
            lsn = assert(nextLsn);
        } while (lsn % this.pageSize == 0);
        return $multi(table.concat(out), lsn);
    }

    /**
     * Appends a record to the log, without flushing it.
     * @param record - The record to append.
     * @returns The record's LSN.
     */
    public appendRecord(record: string): number {
        let remaining = record;
        const lsn = this.getEnd();
        while (true) {
            remaining = this.appendEntry(remaining);
            if (this.isTailFull()) { this.turnTailPage(); } else { break; }
        }
        return lsn;
    }

    /** Closes the log. Using it after this is an error. */
    public close() {
        this.tailPage.append(this.tailBuf);
        this.tailPage.closeAppend();
    }

    public constructor(store: IPageStore<IPage>) {
        this.pageSize = store.pageSize;
        this.entryLengthBytes = math.ceil(math.log(store.pageSize, 256));
        this.entryFmt = "<s" + this.entryLengthBytes;
        this.store = store;

        const pageNums = this.store.listPages();
        if (!next(pageNums)[0]) {
            this.store.getPage(0).create(string.pack(this.entryFmt, ""));
            pageNums.add(0);
        }

        let minPageNum: number = next(pageNums)[0];
        let maxPageNum = minPageNum;
        for (const pageNum of pageNums) {
            if (pageNum > maxPageNum) { maxPageNum = pageNum; }
            if (pageNum < minPageNum) { minPageNum = pageNum; }
        }

        this.headPageNum = minPageNum;
        this.tailPage = this.store.getPage(maxPageNum);
        this.tailSize = assert(this.readPage(this.tailPage)).length;

        // Detect and delete torn records.
        while (true) {
            const tailEntries = this.listEntries(this.tailPage.pageNum);

            let hasTornRecord = false;
            if (this.isTailFull() || tailEntries.length == 0) {
                // The tail page is full or empty, so there's a torn record.
                if (tailEntries.length <= 1) {
                    // The torn record didn't start here, delete the tail page
                    // and continue.
                    const newTailPageNum = this.tailPage.pageNum - 1;
                    this.tailPage.delete();
                    this.tailPage = this.store.getPage(newTailPageNum);
                    this.tailSize = assert(this.readPage(this.tailPage)).length;
                    hasTornRecord = true;
                } else {
                    // The torn record starts here, so throw it away.
                    tailEntries.pop();
                }
            }

            if (!hasTornRecord) {
                // There are no more torn records at this point. We still need
                // to rewrite the entries in case there were torn entries.
                const entryStrs: string[] = [];
                for (const entry of tailEntries) {
                    entryStrs.push(string.pack(this.entryFmt, entry));
                }
                const tailData = table.concat(entryStrs);
                this.tailPage.write(tailData);
                this.tailSize = tailData.length;
                break;
            }
        }

        // We can't set the first LSN to the base of the head because it may
        // point into the middle of a record. We use the next LSN instead.
        const [_, firstLsn] = this.getRecord(this.headPageNum * this.pageSize);
        this.firstLsn = firstLsn;

        this.tailPage.openAppend();
    }
}
