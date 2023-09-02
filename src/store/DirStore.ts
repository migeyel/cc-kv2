import {
    IPage,
    IStoreCollection,
    IPageStore,
    ISerializable,
    Deserializer,
    IAppendableWith,
} from "./IPageStore";

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
export class DirPageDirectory<
    T extends IAppendableWith<A>,
    A extends ISerializable
> implements IStoreCollection<
    T,
    A,
    DirPage<T, A>,
    DirPageStore<T, A>
> {
    public readonly pageSize: number;

    /** Deserializer for the implemented value. */
    private des: Deserializer<T>;

    /** The directory path. */
    private dirPath: string;

    /** The modifications subdir path. */
    private modPath: string;

    public constructor(des: Deserializer<T>, dir: string, pageSize: number) {
        this.des = des;
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

    public getStore(namespace: string): DirPageStore<T, A> {
        return new DirPageStore(
            this.des,
            this.pageSize,
            this.dirPath,
            this.modPath,
            namespace,
        );
    }
}

class DirPageStore<
    T extends IAppendableWith<A>,
    A extends ISerializable
> implements IPageStore<
    T,
    A,
    DirPage<T, A>
> {
    private des: Deserializer<T>;

    public readonly pageSize: number;

    /** The path prefix for files in the store. */
    private filePrefix: string;

    /** The path prefix for modification in the store. */
    private modPrefix: string;

    public constructor(
        des: Deserializer<T>,
        pageSize: number,
        dirPath: string,
        modPath: string,
        namespace: string,
    ) {
        this.des = des;
        this.pageSize = pageSize;
        this.filePrefix = fs.combine(dirPath, namespace + "_");
        this.modPrefix = fs.combine(modPath, namespace + "_");
    }

    public getPage(pageNum: number): DirPage<T, A> {
        return new DirPage(
            this.des,
            this.pageSize,
            this.filePrefix,
            this.modPrefix,
            pageNum,
        );
    }
}

class DirPage<T extends IAppendableWith<A>, A extends ISerializable> implements
IPage<T, A> {
    private des: Deserializer<T>;

    public readonly pageSize: number;

    /** The file path. */
    private filePath: string;

    /** The file modification prefix. */
    private fileModPrefix: string;

    /** The currently open handle. */
    private handle: FileHandle | undefined;

    public constructor(
        des: Deserializer<T>,
        pageSize: number,
        filePrefix: string,
        modPrefix: string,
        pageNum: number,
    ) {
        this.des = des;
        this.pageSize = pageSize;
        this.filePath = filePrefix + tostring(pageNum);
        this.fileModPrefix = modPrefix + tostring(pageNum);
    }

    public exists(): boolean {
        return fs.exists(this.filePath);
    }

    public create(initialData: string): void {
        if (initialData.length == 0) {
            // There's no danger about incomplete writes, so write directly.
            const [file, err] = fs.open(this.filePath, "wb");
            if (!file) { throw err; }
            file.close();
        } else {
            // Mark the data as del so an incomplete write deletes it.
            const delPath = this.fileModPrefix + DEL_SUFFIX;
            const [delFile, err] = fs.open(delPath, "wb");
            if (!delFile) { throw err; }
            delFile.write(initialData);
            delFile.close();

            // Atomic move to the regular path.
            fs.move(delPath, this.filePath);
        }
    }

    public createOpen(): void {
        this.handle = assert(fs.open(this.filePath, "wb")[0]);
    }

    public delete(): void {
        fs.delete(this.filePath);
    }

    public read(): T | undefined {
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
        return this.des.deserialize(out);
    }

    public write(data: T): void {
        // Mark the data as new so an incomplete write ignores it.
        const newPath = this.fileModPrefix + NEW_SUFFIX;
        const [newFile, err] = fs.open(newPath, "wb");
        if (!newFile) { throw err; }
        if (data) { newFile.write(data.serialize()); }
        newFile.close();

        // Atomically move to the regular path.
        fs.delete(this.filePath);
        fs.move(newPath, this.filePath);
    }

    public append(data: A): void {
        this.handle!.write(data.serialize());
    }

    public canAppend(): boolean {
        return this.handle != undefined;
    }

    public openAppend(): void {
        this.handle = assert(fs.open(this.filePath, "ab")[0]);
    }

    public openAppendTruncate(): void {
        this.handle = assert(fs.open(this.filePath, "wb")[0]);
    }

    public closeAppend(): void {
        this.handle!.close();
    }

    public flush(): void { }
}
