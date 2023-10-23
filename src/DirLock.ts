// Exclusive locks on filesystem resources.

/** The timestamp when the computer started. */
const STARTUP_TIMESTAMP = os.epoch("utc") - 1000 * os.clock();

/**
 * Minimum difference in startup timestamps to let the lock acquire. Mostly
 * here because of STARTUP_TIMESTAMP not being 100% accurate.
 */
const TIMESTAMP_DELTA_MS = 16;

/**
 * An exclusive lock stored in a file, to prevent two instances from accessing
 * the same filesystem resources simultaneously.
 */
export default class DirLock {
    private dir: string;
    private held: boolean = true;

    private constructor(dir: string) {
        this.dir = dir;
    }

    /** Tries to acquire the file lock. */
    public static tryAcquire(dir: string): DirLock | undefined {
        const t0 = os.epoch("utc");

        // The shared lock directory. We can only interact with it by moving in
        // a new directory using fs.move (assuming that is atomic).
        const lockDir = fs.combine(dir, "lock");
        const lockFile = fs.combine(lockDir, "lock.bin");

        // Our personal dicectory. We can interact with it as long as we don't
        // yield.
        const myDir = fs.combine(dir, tostring(os.getComputerID()));
        const myFile = fs.combine(myDir, "lock.bin");

        // Set the main directory up and delete any residual myDir.
        fs.makeDir(dir);
        fs.delete(myDir);

        // Check if an older session in our own computer held the lock.
        if (fs.exists(lockDir)) {
            const [f, err] = fs.open(lockFile, "rb");
            if (!f) { throw err; } // lock.bin must exist in the directory.

            const lockStr = f.readAll() || "";
            f.close();

            const [id, timestamp] = string.unpack("<I4I6", lockStr);
            if (id != os.getComputerID()) {
                // Another computer holds the lock.
                // It may have shut down but we have no way to tell.
                return;
            } else if (STARTUP_TIMESTAMP - timestamp < TIMESTAMP_DELTA_MS) {
                // Another program in this computer holds the lock.
                // The computer hasn't shut down since either.
                return;
            } else {
                // This computer held the lock, but it has since shut down.
                // We are free to break it.
                fs.move(lockDir, myDir);
                fs.delete(myDir);
            }
        }

        // Build a new lock so we can try moving it in.
        fs.makeDir(myDir);
        const [f, err] = fs.open(myFile, "wb");
        if (!f) { throw err; }
        f.write(string.pack("<I4I6", os.getComputerID(), STARTUP_TIMESTAMP));
        f.close();

        // Wait for a bit so a sudden reboot doesn't trigger a false positive.
        while (os.epoch("utc") - t0 <= TIMESTAMP_DELTA_MS) { /* empty */ }

        // Try to move it in.
        try {
            fs.move(myDir, lockDir);
        } catch (_) {
            // Another computer moved it in already.
            fs.delete(myDir);
            return;
        }

        return new DirLock(dir);
    }

    /** Releases the file lock, if it is being held. */
    public release() {
        if (this.held) {
            const lockDir = fs.combine(this.dir, "lock");
            const myDir = fs.combine(this.dir, tostring(os.getComputerID()));
            fs.delete(myDir);
            fs.move(lockDir, myDir);
            fs.delete(myDir);
            this.held = false;
        }
    }

    public [Symbol.dispose]() {
        this.release();
    }
}
