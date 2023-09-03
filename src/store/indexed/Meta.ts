import { IPage } from "../IPageStore";

export class IndexMetaPage {
    private page: IPage;

    /** The last procedure to take place in the index, if any. */
    public proc?: Procedure;

    /** Information about substores. */
    public subs: LuaMap<Uuid, SubMeta>;

    public constructor(page: IPage) {
        this.page = page;
        const str = this.page.read();
        if (str) {
            const { proc, subs } = textutils.unserialize(str);
            this.proc = proc;
            this.subs = subs;
        } else {
            this.subs = new LuaMap();
        }
    }

    public commit() {
        this.page.write(textutils.serialize({
            proc: this.proc,
            subs: this.subs,
        }));
        this.page.flush();
    }
}

export type Uuid = string;

export type SubMeta = {
    indexNumber: number,
    numAllocated: number,
}

export type Procedure =
    | PageMoveProcedure
    | PartialPageProcedure

export enum ProcedureType {
    MOVE_PAGE,
    PARTIAL_PAGE,
}

export type PageMoveProcedure = {
    ty: ProcedureType.MOVE_PAGE,
    sourceUuid: string,
    targetUuid: string,
    namespace: string,
    pageNum: number,
}

export type PartialPageProcedure = {
    ty: ProcedureType.PARTIAL_PAGE,
    namespace: string,
    pageNum: number,
}
