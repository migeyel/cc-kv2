const arg: string | undefined = [...$vararg][0];
const arg_module = arg && "src." + arg + ".spec";

assert(arg_module);

// @ts-expect-error: This is a dirty hack to allow module introspection.
const modules: LuaTable<string, unknown> = ____modules;

for (const [module] of modules) {
    if (string.find(module, "%.spec$")[0] && module != "src.main.spec") {
        if (!arg_module || arg_module == module) {
            io.stdout.write(module + "\n");
            const [ok, err] = xpcall(() => require(module), (m) => {
                return debug.traceback(coroutine.running()[0], m, 2);
            });

            if (!ok) {
                printError(tostring(err) + "\n");
                throw err;
            }
        }
    }
}
