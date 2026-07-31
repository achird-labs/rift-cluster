/**
 * Monaco's JSON language contribution has no types of its own.
 *
 * It is imported for its side effect only — registering the `json` language and the worker client
 * that backs it — and the package ships no `.d.ts` for it, so `noImplicitAny` rejects the import
 * without this. Declaring it empty is the honest shape: there is nothing on the module to call.
 *
 * The import exists so `editor.api` can be used instead of `monaco-editor`'s barrel entry. The
 * barrel pulls in every basic language monaco ships (~14 MB of emitted chunks); the console edits
 * JSON and nothing else, and this bundle is embedded byte for byte into the release binary.
 */
declare module "monaco-editor/language/json/monaco.contribution" {}
