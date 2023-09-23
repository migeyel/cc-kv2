export function exit(returnCode?: number) {
    // @ts-expect-error: CraftOS-PC allows shutting down with a return code.
    os.shutdown(returnCode);
}

// @ts-expect-error: This is a dirty hack to allow module introspection.
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
