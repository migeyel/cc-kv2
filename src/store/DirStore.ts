import { IPage, IStoreCollection, IPageStore } from "./IPageStore";

/** The subdirectory for storing store modifications. */
const MOD_SUBDIR = "_";

/** The modification suffix for signalling deletion. */
const DEL_SUFFIX = "_DEL";

function matchDel(modfile: string): string | undefined {
    return string.match("(.+)_DEL$", modfile)[0];
}

/** The modification suffix for signalling updates. */
const NEW_SUFFIX = "_NEW";

function matchNew(modfile: string): string | undefined {
    return string.match("(.+)_NEW$", modfile)[0];
}

/**
 * A simple store collection, stored together in a single filesystem directory.
 */
export class DirStoreCollection implements IStoreCollection<
    DirPage,
    DirPageStore
> {
    public readonly pageSize: number;

    /** The directory path. */
    private dirPath: string;

    /** The modifications subdir path. */
    private modPath: string;

    public constructor(dir: string, pageSize: number) {
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

    public getStore(namespace: string): DirPageStore {
        return new DirPageStore(
            this.pageSize,
            this.dirPath,
            this.modPath,
            namespace,
        );
    }

    public listStores(): LuaSet<string> {
        const out = new LuaSet<string>();
        for (const file of fs.find(this.dirPath + "/*")) {
            const [name] = string.match(file, "^(.*)_[0-9]+$");
            if (name) { out.add(name); }
        }
        return out;
    }
}

class DirPageStore implements IPageStore<DirPage> {
    public readonly pageSize: number;

    /** The path prefix for files in the store. */
    private filePrefix: string;

    /** The path prefix for modification in the store. */
    private modPrefix: string;

    public constructor(
        pageSize: number,
        dirPath: string,
        modPath: string,
        namespace: string,
    ) {
        this.pageSize = pageSize;
        this.filePrefix = fs.combine(dirPath, namespace + "_");
        this.modPrefix = fs.combine(modPath, namespace + "_");
    }

    public getPage(pageNum: number): DirPage {
        return new DirPage(
            this.pageSize,
            this.filePrefix,
            this.modPrefix,
            pageNum,
        );
    }

    public listPages(): LuaSet<number> {
        const out = new LuaSet<number>();
        for (const file of fs.find(this.filePrefix + "*")) {
            out.add(assert(tonumber(string.match(file, "_[0-9]+$"))));
        }
        return out;
    }
}

class DirPage implements IPage {
    public readonly pageSize: number;

    public readonly pageNum: number;

    /** The file path. */
    private filePath: string;

    /** The file modification prefix. */
    private fileModPrefix: string;

    /** The currently open handle. */
    private handle: FileHandle | undefined;

    public constructor(
        pageSize: number,
        filePrefix: string,
        modPrefix: string,
        pageNum: number,
    ) {
        this.pageNum = pageNum;
        this.pageSize = pageSize;
        this.filePath = filePrefix + tostring(pageNum);
        this.fileModPrefix = modPrefix + tostring(pageNum);
    }

    public exists(): boolean {
        return fs.exists(this.filePath);
    }

    public create(initialData?: string): void {
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
        this.handle = assert(fs.open(this.filePath, "wb")[0]);
    }

    public delete(): void {
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
        this.handle!.write(data);
    }

    public canAppend(): boolean {
        return this.handle != undefined;
    }

    public openAppend(): void {
        this.handle = assert(fs.open(this.filePath, "ab")[0]);
    }

    public closeAppend(): void {
        this.handle!.close();
    }

    public flush(): void { }
}
