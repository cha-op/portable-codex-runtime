const fakePgUrl = new URL("./fake-pg.mjs", import.meta.url).href;

export async function resolve(specifier, context, nextResolve) {
  if (specifier === "pg") {
    return { format: "module", shortCircuit: true, url: fakePgUrl };
  }
  return nextResolve(specifier, context);
}
