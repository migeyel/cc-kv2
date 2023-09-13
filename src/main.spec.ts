export function exit(returnCode?: number) {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    os.shutdown(returnCode);
}

// HACK
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
const modules: LuaTable<string, unknown> = ____modules;

for (const [module] of modules) {
    if (string.find(module, "%.spec$")[0] && module != "main.spec") {
        io.stdout.write(module + "\n");
        const [ok, err] = xpcall(() => require(module), (m) => {
            return debug.traceback(coroutine.running()[0], m, 2);
        });

        if (!ok) {
            io.stderr.write(err + "\n");
            exit(1);
        }
    }
}

exit(0);
