import { ConfigEntryComponent } from "./ConfigPageComponent";
import { Namespace, PageNum } from "./store/IPageStore";
import { PAGE_FMT } from "./txStore/LogRecord/types";
import { IEvent, IObj, TxCollection, TxPage } from "./txStore/LogStore";

/**
 * A component for allocating and deallocating pages.
 *
 * Remark: this allocator treats empty page objects as "unallocated". Therefore,
 * you mustn't rely on allocated empty objects staying empty. As soon as an
 * object managed by this allocator is made empty it must be considered free.
 */
export class PageAllocatorComponent {
    private numPagesConfig: ConfigEntryComponent;

    public readonly pagesNamespace: Namespace;

    public constructor(
        numPagesConfig: ConfigEntryComponent,
        pagesNamespace: Namespace,
    ) {
        this.pagesNamespace = pagesNamespace;
        this.numPagesConfig = numPagesConfig;
    }

    private getNumPages(cl: TxCollection): number {
        const str = this.numPagesConfig.get(cl);
        if (!str) { return 0; }
        return string.unpack("<" + PAGE_FMT, str)[0];
    }

    private setNumPages(cl: TxCollection, numPages: number) {
        this.numPagesConfig.set(cl, string.pack("<" + PAGE_FMT, numPages));
    }

    /** Allocates a new page and returns it. */
    public allocPageCast<
        T extends IObj<E>,
        E extends IEvent
    >(cl: TxCollection): TxPage<T, E> {
        let numPages = this.getNumPages(cl);
        const ns = cl.getStoreCast<T, E>(this.pagesNamespace);

        let attemptPage = ns.getPage(math.random(0, numPages) as PageNum);
        while (!attemptPage.obj.isEmpty()) {
            attemptPage = ns.getPage(numPages as PageNum);
            numPages++;
        }

        this.setNumPages(cl, numPages);
        return attemptPage;
    }

    /**
     * Shrink the page search space to match the existing pages.
     * @param cl - The collection to operate on.
     * @param hint - A hint to a page which was just freed, for performance.
     */
    public freeUnusedPages(cl: TxCollection, hint?: PageNum): void {
        let numPages = this.getNumPages(cl);
        if (hint && hint != numPages - 1) { return; }
        const ns = cl.getStoreCast<IObj<IEvent>, IEvent>(this.pagesNamespace);
        while (numPages > 0) {
            const page = ns.getPage(numPages - 1 as PageNum);
            if (page.obj.isEmpty()) { numPages--; } else { break; }
        }
        this.setNumPages(cl, numPages);
    }

    public deserializeObj(n: Namespace, s?: string): IObj<IEvent> | undefined {
        return this.numPagesConfig.deserializeObj(n, s);
    }

    public deserializeEv(n: Namespace, s: string): IEvent | undefined {
        return this.numPagesConfig.deserializeEv(n, s);
    }
}
