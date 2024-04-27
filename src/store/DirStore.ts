import { ShMap } from "../ShMap";
import {
    IPage,
    IStoreCollection,
    IPageStore,
    MAX_NAMESPACE,
    Namespace,
    PageSize,
    PageNum,
} from "./IPageStore";

/** The subdirectory for storing store modifications. */
const MOD_SUBDIR = "_";

/** The modification suffix for signalling deletion. */
const DEL_SUFFIX = "_DEL";

function matchDel(modfile: string): string | undefined {
    return string.match(modfile, "(.+)_DEL$")[0];
}

/** The modification suffix for signalling updates. */
const NEW_SUFFIX = "_NEW";

function matchNew(modfile: string): string | undefined {
    return string.match(modfile, "(.+)_NEW$")[0];
}

/**
 * A simple store collection, stored together in a single filesystem directory.
 */
export class DirStoreCollection implements IStoreCollection<
    DirPage,
    DirPageStore
> {
    public readonly pageSize: PageSize;

    /** The directory path. */
    private dirPath: string;

    /** The modifications subdir path. */
    private modPath: string;

    /** The shared store map. */
    private map = new ShMap<DirPage, DirPageStore>();

    public constructor(dir: string, pageSize: PageSize) {
        this.pageSize = pageSize;
        this.dirPath = dir;
        this.modPath = fs.combine(dir, MOD_SUBDIR);

        fs.makeDir(this.dirPath);
        fs.makeDir(this.modPath);

        // Recover from partial modifications.
        for (const filename of fs.list(this.modPath)) {
            const fileModPath = fs.combine(this.modPath, filename);

            // Del files get deleted.
            const delFile = matchDel(filename);
            if (delFile) { fs.delete(fileModPath); }

            // New files take the older version.
            const newFile = matchNew(filename);
            if (newFile) {
                const oldPath = fs.combine(this.dirPath, newFile);
                if (fs.exists(oldPath)) {
                    fs.delete(fileModPath);
                } else {
                    fs.move(fileModPath, oldPath);
                }
            }
        }
    }

    public getStore(namespace: Namespace): DirPageStore {
        assert(namespace <= MAX_NAMESPACE);
        return this.map.getStore(namespace, () => new DirPageStore(
            this.map,
            this.pageSize,
            this.dirPath,
            this.modPath,
            namespace,
        ));
    }

    public listStores(): LuaSet<Namespace> {
        const out = new LuaSet<Namespace>();
        for (const path of fs.find(this.dirPath + "/*")) {
            const file = fs.getName(path);
            const name = tonumber(string.match(file, "^([0-9]+)_[0-9]+$")[0]);
            if (name != undefined) { out.add(name as Namespace); }
        }
        return out;
    }
}

class DirPageStore implements IPageStore<DirPage> {
    public readonly pageSize: PageSize;

    public readonly namespace: Namespace;

    /** The path prefix for files in the store. */
    private filePrefix: string;

    /** The path prefix for modification in the store. */
    private modPrefix: string;

    /** The shared page map. */
    private map: ShMap<DirPage, DirPageStore>;

    public constructor(
        map: ShMap<DirPage, DirPageStore>,
        pageSize: PageSize,
        dirPath: string,
        modPath: string,
        namespace: Namespace,
    ) {
        this.map = map;
        this.pageSize = pageSize;
        this.namespace = namespace;
        this.filePrefix = fs.combine(dirPath, namespace + "_");
        this.modPrefix = fs.combine(modPath, namespace + "_");
    }

    public getPage(pageNum: PageNum): DirPage {
        return this.map.getPage(this.namespace, pageNum, () => new DirPage(
            this.pageSize,
            this.namespace,
            this.filePrefix,
            this.modPrefix,
            pageNum,
        ));
    }

    public listPages(): LuaSet<PageNum> {
        const out = new LuaSet<PageNum>();
        for (const path of fs.find(this.filePrefix + "*")) {
            const file = fs.getName(path);
            const match = string.match(file, "_([0-9]+)$")[0];
            const pageNum = assert(tonumber(match)) as PageNum;
            if (match != undefined) { out.add(pageNum); }
        }
        return out;
    }
}

class DirPage implements IPage {
    public readonly pageSize: PageSize;

    public readonly namespace: Namespace;

    public readonly pageNum: PageNum;

    /** The file path. */
    private filePath: string;

    /** The file modification prefix. */
    private fileModPrefix: string;

    /** The currently open handle. */
    private handle?: FileHandle;

    public constructor(
        pageSize: PageSize,
        namespace: Namespace,
        filePrefix: string,
        modPrefix: string,
        pageNum: PageNum,
    ) {
        this.pageNum = pageNum;
        this.namespace = namespace;
        this.pageSize = pageSize;
        this.filePath = filePrefix + tostring(pageNum);
        this.fileModPrefix = modPrefix + tostring(pageNum);
    }

    public exists(): boolean {
        return fs.exists(this.filePath);
    }

    public create(initialData?: string): void {
        assert(!this.handle);
        if (initialData) {
            // Mark the data as del so an incomplete write deletes it.
            const delPath = this.fileModPrefix + DEL_SUFFIX;
            const [delFile, err] = fs.open(delPath, "wb");
            if (!delFile) { throw err; }
            delFile.write(initialData);
            delFile.close();

            // Atomic move to the regular path.
            fs.move(delPath, this.filePath);
        } else {
            // There's no danger about incomplete writes, so write directly.
            const [file, err] = fs.open(this.filePath, "wb");
            if (!file) { throw err; }
            file.close();
        }
    }

    public createOpen(): void {
        assert(!this.handle);
        this.handle = assert(fs.open(this.filePath, "wb")[0]);
    }

    public delete(): void {
        if (this.handle) { this.closeAppend(); }
        fs.delete(this.filePath);
    }

    public read(): string | undefined {
        const [file, err] = fs.open(this.filePath, "rb");
        if (!file) {
            if (fs.exists(this.filePath)) {
                throw err;
            } else {
                return;
            }
        }
        const out = file.readAll() || "";
        file.close();
        return out;
    }

    public write(data: string): void {
        assert(!this.handle);

        // Mark the data as new so an incomplete write ignores it.
        const newPath = this.fileModPrefix + NEW_SUFFIX;
        const [newFile, err] = fs.open(newPath, "wb");
        if (!newFile) { throw err; }
        newFile.write(data);
        newFile.close();

        // Atomically move to the regular path.
        fs.delete(this.filePath);
        fs.move(newPath, this.filePath);
    }

    public append(data: string): void {
        const handle = assert(this.handle);
        handle.write(data);
        handle.flush();
    }

    public canAppend(): boolean {
        return this.handle != undefined;
    }

    public openAppend(): void {
        assert(!this.handle);
        this.handle = assert(fs.open(this.filePath, "ab")[0]);
    }

    public closeAppend(): void {
        assert(this.handle).close();
        this.handle = undefined;
    }
}
