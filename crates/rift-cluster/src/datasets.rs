//! Binding a stub response to a dataset (RFC-005 D2, issue #286).
//!
//! Two transforms, both pure, both over `serde_json::Value`:
//!
//! * [`pin_bindings`] runs **once, on the leader, at admission**. It resolves each
//!   `_rift.dataset` block's version to a concrete `(version, digest)` and writes both back, so
//!   the pin rides the committed entry.
//! * [`compile_bindings`] runs **on every node, at apply**. It rewrites each pinned block into the
//!   `_behaviors.lookup` the engine already executes, pointing at this node's spool file.
//!
//! Splitting it this way is the determinism argument. The spool path is node-local, so it cannot
//! travel in the log; "latest version" is time-dependent, so it must not be re-resolved at apply.
//! Pinning the digest on the leader and keying only on it at apply leaves apply a pure function of
//! the committed entry — every node compiles the same file, whenever it applies.
//!
//! Both work on JSON rather than a parsed `ImposterConfig`, which is not a style choice: upstream
//! parses `_behaviors` **once at construction** into a private `behaviors_parsed` (issue #479's
//! hot-path precompute), so mutating a parsed config would update the JSON field and leave what
//! the engine actually executes unchanged — a silent no-op. Staying in JSON also keeps both
//! transforms testable with no redb, no engine and no fleet.

use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

/// One dataset version, as the caller's table read resolves it.
///
/// Carries `key_columns` because admission refuses a `keyColumn` the dataset does not *declare* —
/// that refusal plus #285's duplicate-key validation is what makes a bound lookup single-valued.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDataset {
    pub version: u64,
    pub digest: String,
    pub delimiter: char,
    pub key_columns: Vec<String>,
}

/// Every response object in `document` that carries a `_rift.dataset` block.
///
/// A **shape-agnostic** search rather than a walk of `stubs[].responses[]`, because a binding
/// reaches the admission path through five different body shapes — a whole `ImposterConfig`, a
/// replace-all payload, an add-stub body, a replace-stubs body, and a bare stub. Keying on the
/// block itself covers all five and any future one, where a shape-specific walk would silently
/// skip the bodies nobody remembered to teach it about — and a skipped binding is not an error,
/// it is an unpinned block that reaches apply and refuses there instead.
///
/// Order is unspecified: each binding is transformed independently of the others.
///
/// An object whose `_rift.dataset` is not itself an object is not yielded — it cannot be a
/// binding, and `serde` refuses it when the document is parsed. Duplicating that error here would
/// only make the message worse.
fn bindings_of(document: &mut Value) -> Vec<&mut Value> {
    let mut found = Vec::new();
    let mut stack = vec![document];
    while let Some(node) = stack.pop() {
        if node
            .get("_rift")
            .and_then(|rift| rift.get("dataset"))
            .is_some_and(Value::is_object)
        {
            found.push(node);
            continue;
        }
        match node {
            Value::Array(items) => stack.extend(items.iter_mut()),
            Value::Object(fields) => stack.extend(fields.values_mut()),
            _ => {}
        }
    }
    found
}

/// The dataset names bound anywhere in `document`, in unspecified order.
///
/// For callers that must *notice* a binding without being able to pin one — a source pull, whose
/// content is fetched rather than admitted. An unpinned block that reaches storage is refused at
/// apply, and a refusal there aborts the whole engine sync, so a document from a remote endpoint
/// could otherwise freeze a node's config plane.
#[must_use]
pub fn bound_dataset_names(document: &Value) -> Vec<String> {
    let mut names = Vec::new();
    let mut stack = vec![document];
    while let Some(node) = stack.pop() {
        if let Some(binding) = node.get("_rift").and_then(|rift| rift.get("dataset"))
            && binding.is_object()
        {
            names.push(
                binding
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("<unnamed>")
                    .to_owned(),
            );
            continue;
        }
        match node {
            Value::Array(items) => stack.extend(items.iter()),
            Value::Object(fields) => stack.extend(fields.values()),
            _ => {}
        }
    }
    names
}

/// A required string field of a binding.
fn field<'a>(binding: &'a Value, key: &str, name: &str) -> Result<&'a str, String> {
    binding
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("dataset binding {name} is missing a string `{key}`"))
}

/// Resolve and pin every `_rift.dataset` block — **leader, at admission**.
///
/// `resolve` is given the dataset's name and the requested version (`None` for "latest live") and
/// answers the concrete version it resolved to. Pinning writes both the version and its digest
/// back into the block, so what commits names an exact set of bytes rather than a moving target.
///
/// Refuses, naming what is wrong, when the dataset is absent or tombstoned, or when `keyColumn` is
/// not one the dataset *declares*. That second refusal is half the determinism argument: #285
/// proves every declared key column is unique across rows, so a binding restricted to them matches
/// at most one row. An undeclared column carries no such proof and the engine picks among
/// duplicates in hash order.
///
/// Deliberately does **not** compile the block — the spool path is node-local, so there is nothing
/// correct to compile here, and the stored record is what `GET` renders back to the operator.
pub fn pin_bindings(
    config: &mut Value,
    mut resolve: impl FnMut(&str, Option<u64>) -> Option<ResolvedDataset>,
) -> Result<(), String> {
    for response in bindings_of(config) {
        let binding = &response["_rift"]["dataset"];
        let name = field(binding, "name", "<unnamed>")?.to_owned();
        let key_column = field(binding, "keyColumn", &name)?.to_owned();
        let requested = binding.get("version").and_then(Value::as_u64);

        let resolved = resolve(&name, requested).ok_or_else(|| match requested {
            Some(v) => format!("dataset \"{name}\" has no live version {v}"),
            None => format!("dataset \"{name}\" does not exist"),
        })?;

        if !resolved.key_columns.iter().any(|c| c == &key_column) {
            return Err(format!(
                "dataset \"{name}\" does not declare key column \"{key_column}\" (declared: {})",
                resolved.key_columns.join(", ")
            ));
        }

        let binding = &mut response["_rift"]["dataset"];
        binding["version"] = Value::from(resolved.version);
        binding["digest"] = Value::String(resolved.digest);
    }
    Ok(())
}

/// Rewrite every pinned `_rift.dataset` into the engine's own `lookup` — **every node, at apply**.
///
/// Keys **only** on the committed pin. Re-resolving "latest" here is the one thing that would
/// break determinism: two nodes applying the same entry at different times could reach different
/// versions. `resolve` is therefore asked for an exact `(name, version)`, and its digest must
/// agree with the pinned one — `(name, version)` is immutable once committed, since every upload
/// is a new version, so a disagreement is committed-state corruption rather than an input.
///
/// The compiled entry is appended to the response's existing behaviours rather than replacing
/// them, and the declarative block is left in place: the engine ignores it, and removing it would
/// only make the engine-facing copy differ from the stored one for no gain.
pub fn compile_bindings(
    config: &mut Value,
    mut resolve: impl FnMut(&str, u64) -> Result<Option<ResolvedDataset>, String>,
    spool_dir: Option<&Path>,
) -> Result<(), String> {
    for response in bindings_of(config) {
        let binding = &response["_rift"]["dataset"];
        let name = field(binding, "name", "<unnamed>")?.to_owned();
        let key_column = field(binding, "keyColumn", &name)?.to_owned();
        let into = field(binding, "into", &name)?.to_owned();
        let key = binding
            .get("key")
            .cloned()
            .ok_or_else(|| format!("dataset binding {name} is missing `key`"))?;

        // Absent means the block never went through admission. Resolving it here would be the
        // non-determinism this split exists to prevent, so it is a refusal rather than a fallback.
        let (Some(version), Some(digest)) = (
            binding.get("version").and_then(Value::as_u64),
            binding
                .get("digest")
                .and_then(Value::as_str)
                .map(str::to_owned),
        ) else {
            return Err(format!(
                "dataset binding \"{name}\" reached apply without a pin (no version/digest); \
                 it was never admitted"
            ));
        };

        let spool = spool_dir.ok_or_else(|| {
            format!(
                "dataset binding \"{name}\" needs a dataset spool directory; this node has none"
            )
        })?;

        // `Err` is a storage failure or a corrupt row; `Ok(None)` is a genuinely absent one.
        // Collapsing the two would report committed-state corruption as a phantom deletion, and
        // the operator would chase a version bug instead of the disk.
        let resolved = resolve(&name, version)?
            .ok_or_else(|| format!("dataset \"{name}\" version {version} is not present"))?;
        if resolved.digest != digest {
            return Err(format!(
                "dataset \"{name}\" version {version} holds digest {} but the binding pinned \
                 {digest}",
                resolved.digest
            ));
        }

        let entry = serde_json::json!({
            "key": key,
            "fromDataSource": { "csv": {
                "path": spool.join(format!("{digest}.csv")).to_string_lossy(),
                "keyColumn": key_column,
                "delimiter": resolved.delimiter.to_string(),
            } },
            "into": into,
        });

        // Append into this response's behaviours. Both spellings and **both shapes** occur, and
        // the shape apply actually sees is the array one: a stored config is
        // `serde_json::to_string(&ImposterConfig)`, and upstream's `StubResponseOut.behaviors` is
        // `Option<Vec<Value>>`. Treating only the object shape refused every bound stub that also
        // carried any other behaviour, and a refusal here aborts the *whole* engine sync — one
        // ordinary `POST /imposters` would have frozen the config plane on every node.
        let slot = ["_behaviors", "behaviors"]
            .into_iter()
            .find(|k| response.get(*k).is_some())
            .unwrap_or("_behaviors");
        let block = response
            .as_object_mut()
            .ok_or_else(|| format!("response carrying dataset \"{name}\" is not an object"))?
            .entry(slot)
            .or_insert_with(|| Value::Object(Map::new()));
        match block {
            Value::Object(fields) => push_lookup(
                fields
                    .entry("lookup")
                    .or_insert_with(|| Value::Array(Vec::new())),
                entry,
                &name,
                slot,
            )?,
            // Upstream's `normalize_behaviors` merges array elements by key, **last wins**, so a
            // second `{"lookup": …}` element would silently clobber a hand-written one rather than
            // adding to it. Push into the element that already carries `lookup`; only when none
            // does is a new element correct.
            Value::Array(items) => match items.iter_mut().find(|i| i.get("lookup").is_some()) {
                Some(existing) => push_lookup(
                    existing
                        .as_object_mut()
                        .ok_or_else(|| {
                            format!("a `{slot}` element on the response carrying \"{name}\" is not an object")
                        })?
                        .entry("lookup")
                        .or_insert_with(|| Value::Array(Vec::new())),
                    entry,
                    &name,
                    slot,
                )?,
                None => items.push(serde_json::json!({ "lookup": [entry] })),
            },
            _ => {
                return Err(format!(
                    "`{slot}` on the response carrying \"{name}\" is neither an object nor an array"
                ));
            }
        }
    }
    Ok(())
}

/// Push one compiled entry into a `lookup` slot that must be an array.
fn push_lookup(lookups: &mut Value, entry: Value, name: &str, slot: &str) -> Result<(), String> {
    lookups
        .as_array_mut()
        .ok_or_else(|| {
            format!("`{slot}.lookup` on the response carrying \"{name}\" is not an array")
        })?
        .push(entry);
    Ok(())
}

/// Undo [`compile_bindings`] for a rendered document — **the read path**.
///
/// The engine holds the compiled form, so anything rendered *from* the engine carries a spool
/// path. That path is node-local, which makes it wrong twice over on the way out: the same
/// imposter renders differently depending on which node answered, and an operator who edits a
/// rendered document and `PUT`s it back would store a filesystem path that is correct only on the
/// node that produced it. Stripping it leaves the operator's own `_rift.dataset` block, which is
/// the thing they wrote and the thing they can act on.
///
/// Only the entry this node compiled is removed — matched by the exact spool path for that
/// response's own pinned digest. A hand-written `lookup` with an explicit path is left alone,
/// which is the whole of the `--cluster`-off compatibility promise.
pub fn strip_compiled(document: &mut Value, spool_path: impl Fn(&str) -> Option<PathBuf>) {
    for response in bindings_of(document) {
        let Some(digest) = response["_rift"]["dataset"]
            .get("digest")
            .and_then(Value::as_str)
        else {
            continue;
        };
        let Some(compiled) = spool_path(digest) else {
            continue;
        };
        let compiled = compiled.to_string_lossy().into_owned();

        for slot in ["_behaviors", "behaviors"] {
            let Some(block) = response.get_mut(slot) else {
                continue;
            };
            // Two shapes, because the one that goes in is not the one that comes back: a config is
            // written with `behaviors` as an object, and the engine renders it as an *array* of
            // behavior objects. Handling only the written shape means the strip silently does
            // nothing on exactly the documents it exists to clean.
            match block {
                Value::Array(entries) => {
                    for entry in entries.iter_mut() {
                        strip_lookup(entry, &compiled);
                    }
                    entries.retain(|entry| !is_empty_object(entry));
                }
                Value::Object(_) => strip_lookup(block, &compiled),
                _ => continue,
            }
            if (is_empty_object(block) || matches!(block, Value::Array(e) if e.is_empty()))
                && let Some(fields) = response.as_object_mut()
            {
                fields.remove(slot);
            }
        }
    }
}

/// Drop the `lookup` entry pointing at `compiled` from one behaviours object.
///
/// An emptied `lookup` array is removed rather than left behind: `"lookup": []` on a response the
/// operator never gave one to reads as a behaviour they configured and did not, and would come
/// back on the next `PUT`.
fn strip_lookup(block: &mut Value, compiled: &str) {
    let Some(lookups) = block.get_mut("lookup").and_then(Value::as_array_mut) else {
        return;
    };
    lookups.retain(|entry| entry["fromDataSource"]["csv"]["path"].as_str() != Some(compiled));
    if lookups.is_empty()
        && let Some(fields) = block.as_object_mut()
    {
        fields.remove("lookup");
    }
}

fn is_empty_object(value: &Value) -> bool {
    value.as_object().is_some_and(serde_json::Map::is_empty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dataset(version: u64, digest: &str) -> ResolvedDataset {
        ResolvedDataset {
            version,
            digest: digest.to_owned(),
            delimiter: ',',
            key_columns: vec!["id".to_owned()],
        }
    }

    /// A config with one response carrying `binding` as its `_rift.dataset`.
    fn config_with(binding: Value) -> Value {
        json!({
            "port": 4545,
            "protocol": "http",
            "stubs": [{
                "id": "lookup",
                "responses": [{
                    "is": { "statusCode": 200, "body": "${row}[name]" },
                    "_rift": { "dataset": binding }
                }]
            }]
        })
    }

    fn binding() -> Value {
        json!({
            "name": "customers",
            "key": { "from": { "query": "id" }, "using": { "method": "regex", "selector": ".*" } },
            "keyColumn": "id",
            "into": "${row}"
        })
    }

    fn block(config: &Value) -> &Value {
        &config["stubs"][0]["responses"][0]["_rift"]["dataset"]
    }

    fn lookups(config: &Value) -> &Value {
        &config["stubs"][0]["responses"][0]["_behaviors"]["lookup"]
    }

    /// E1 — an unpinned block takes the latest live version, and records the digest with it.
    #[test]
    fn an_unpinned_binding_pins_the_latest_version_and_its_digest() {
        let mut config = config_with(binding());
        pin_bindings(&mut config, |name, version| {
            assert_eq!(name, "customers");
            assert_eq!(version, None, "an unpinned block must ask for the latest");
            Some(dataset(7, "digest-7"))
        })
        .expect("pins");

        assert_eq!(block(&config)["version"], json!(7));
        assert_eq!(block(&config)["digest"], json!("digest-7"));
    }

    /// E2 — an explicit version is the one pinned, even when a later one exists.
    #[test]
    fn an_explicit_version_is_pinned_not_the_latest() {
        let mut b = binding();
        b["version"] = json!(3);
        let mut config = config_with(b);

        pin_bindings(&mut config, |_, version| {
            assert_eq!(
                version,
                Some(3),
                "an explicit version must be asked for by number"
            );
            Some(dataset(3, "digest-3"))
        })
        .expect("pins");

        assert_eq!(block(&config)["version"], json!(3));
        assert_eq!(block(&config)["digest"], json!("digest-3"));
    }

    /// E4 — an absent dataset is refused, and the message names it.
    #[test]
    fn binding_an_absent_dataset_is_refused_by_name() {
        let mut config = config_with(binding());
        let err = pin_bindings(&mut config, |_, _| None).expect_err("must refuse");
        assert!(
            err.contains("customers"),
            "the refusal must name the dataset: {err}"
        );
    }

    /// E6 — a `keyColumn` the dataset does not declare is refused, naming the column.
    ///
    /// This is half the determinism argument: #285 proves every *declared* key column is unique
    /// across rows, so a binding restricted to those columns matches at most one row. An
    /// undeclared column carries no such proof, and the engine picks among duplicates in hash
    /// order — which is exactly the non-determinism this issue exists to make unrepresentable.
    #[test]
    fn binding_an_undeclared_key_column_is_refused_by_name() {
        let mut b = binding();
        b["keyColumn"] = json!("email");
        let mut config = config_with(b);

        let err =
            pin_bindings(&mut config, |_, _| Some(dataset(1, "d1"))).expect_err("must refuse");
        assert!(
            err.contains("email"),
            "the refusal must name the column: {err}"
        );
    }

    /// E11 — a config with no binding is returned untouched.
    ///
    /// Asserted by whole-value equality rather than by spot-checking fields: the guarantee is that
    /// the overwhelmingly common config is not perturbed *at all*, and a transform that quietly
    /// added an empty `_behaviors` to every response would pass a narrower check.
    #[test]
    fn a_config_with_no_binding_is_untouched_by_both_transforms() {
        let plain = json!({
            "port": 4545,
            "protocol": "http",
            "stubs": [{ "responses": [{ "is": { "statusCode": 200 } }] }]
        });

        let mut pinned = plain.clone();
        pin_bindings(&mut pinned, |_, _| panic!("must not resolve anything")).expect("no-op");
        assert_eq!(pinned, plain);

        let mut compiled = plain.clone();
        compile_bindings(
            &mut compiled,
            |_, _| panic!("must not resolve anything"),
            Some(Path::new("/spool")),
        )
        .expect("no-op");
        assert_eq!(compiled, plain);
    }

    /// E7 — compile-down points at the file named by the **pinned digest**.
    #[test]
    fn compiling_uses_the_pinned_digest_for_the_spool_path() {
        let mut b = binding();
        b["version"] = json!(3);
        b["digest"] = json!("digest-3");
        let mut config = config_with(b);

        compile_bindings(
            &mut config,
            |_, version| {
                assert_eq!(
                    version, 3,
                    "apply must key on the pinned version, never 'latest'"
                );
                Ok(Some(dataset(3, "digest-3")))
            },
            Some(Path::new("/spool")),
        )
        .expect("compiles");

        assert_eq!(
            lookups(&config)[0]["fromDataSource"]["csv"]["path"],
            json!("/spool/digest-3.csv")
        );
        assert_eq!(
            lookups(&config)[0]["fromDataSource"]["csv"]["keyColumn"],
            json!("id")
        );
        assert_eq!(lookups(&config)[0]["into"], json!("${row}"));
        assert_eq!(
            lookups(&config)[0]["key"],
            json!({ "from": { "query": "id" }, "using": { "method": "regex", "selector": ".*" } }),
            "the key extraction carries across unchanged"
        );
    }

    /// E13 — the delimiter is the dataset's, not an assumed comma.
    ///
    /// A dataset uploaded with `;` compiled against `,` reads the whole line as one column, so
    /// every `${row}[col]` silently resolves to nothing. Defaulting here would be wrong quietly.
    #[test]
    fn the_compiled_delimiter_comes_from_the_dataset_not_a_default() {
        let mut b = binding();
        b["version"] = json!(1);
        b["digest"] = json!("d1");
        let mut config = config_with(b);

        compile_bindings(
            &mut config,
            |_, _| {
                Ok(Some(ResolvedDataset {
                    version: 1,
                    digest: "d1".to_owned(),
                    delimiter: ';',
                    key_columns: vec!["id".to_owned()],
                }))
            },
            Some(Path::new("/spool")),
        )
        .expect("compiles");

        assert_eq!(
            lookups(&config)[0]["fromDataSource"]["csv"]["delimiter"],
            json!(";")
        );
    }

    /// E8 — an existing `_behaviors` block survives; the lookup joins it.
    #[test]
    fn compiling_preserves_an_existing_behaviours_block() {
        let mut b = binding();
        b["version"] = json!(1);
        b["digest"] = json!("d1");
        let mut config = config_with(b);
        config["stubs"][0]["responses"][0]["_behaviors"] = json!({ "wait": 50 });

        compile_bindings(
            &mut config,
            |_, _| Ok(Some(dataset(1, "d1"))),
            Some(Path::new("/spool")),
        )
        .expect("compiles");

        assert_eq!(
            config["stubs"][0]["responses"][0]["_behaviors"]["wait"],
            json!(50),
            "an unrelated behavior must not be clobbered"
        );
        assert_eq!(lookups(&config).as_array().expect("array").len(), 1);
    }

    /// E9 — an existing hand-written `lookup` is appended to, never replaced.
    #[test]
    fn compiling_appends_to_an_existing_lookup_array() {
        let mut b = binding();
        b["version"] = json!(1);
        b["digest"] = json!("d1");
        let mut config = config_with(b);
        let hand_written = json!({
            "key": { "from": { "query": "x" }, "using": { "method": "regex", "selector": ".*" } },
            "fromDataSource": { "csv": { "path": "/etc/other.csv", "keyColumn": "x" } },
            "into": "${other}"
        });
        config["stubs"][0]["responses"][0]["_behaviors"] =
            json!({ "lookup": [hand_written.clone()] });

        compile_bindings(
            &mut config,
            |_, _| Ok(Some(dataset(1, "d1"))),
            Some(Path::new("/spool")),
        )
        .expect("compiles");

        let entries = lookups(&config).as_array().expect("array");
        assert_eq!(entries.len(), 2, "the hand-written entry must survive");
        assert_eq!(entries[0], hand_written);
        assert_eq!(
            entries[1]["fromDataSource"]["csv"]["path"],
            json!("/spool/d1.csv")
        );
    }

    /// E10 — a raw `lookup` with an explicit path and no binding is left exactly as written.
    ///
    /// AC4's upstream-compatibility promise: `--cluster` changes nothing for a config that never
    /// asked for a dataset.
    #[test]
    fn a_raw_lookup_with_an_explicit_path_is_untouched() {
        let raw = json!({
            "port": 4545,
            "protocol": "http",
            "stubs": [{
                "responses": [{
                    "is": { "statusCode": 200 },
                    "_behaviors": { "lookup": [{
                        "key": { "from": { "query": "id" }, "using": { "method": "regex", "selector": ".*" } },
                        "fromDataSource": { "csv": { "path": "/etc/rows.csv", "keyColumn": "id" } },
                        "into": "${row}"
                    }] }
                }]
            }]
        });

        let mut config = raw.clone();
        compile_bindings(
            &mut config,
            |_, _| panic!("must not resolve"),
            Some(Path::new("/spool")),
        )
        .expect("no-op");
        assert_eq!(config, raw);
    }

    /// E12 — a node with no spool directory refuses rather than compiling a bogus path.
    ///
    /// The alternative — emitting a path that resolves nowhere — is served as a 200 whose
    /// `${row}` tokens are never substituted, i.e. the wrong body under a success status.
    #[test]
    fn compiling_without_a_spool_directory_is_refused() {
        let mut b = binding();
        b["version"] = json!(1);
        b["digest"] = json!("d1");
        let mut config = config_with(b);

        let err = compile_bindings(&mut config, |_, _| Ok(Some(dataset(1, "d1"))), None)
            .expect_err("must refuse");
        assert!(
            err.contains("spool"),
            "the refusal must say what is missing: {err}"
        );
    }

    /// An unpinned block reaching apply is a refusal, not a re-resolution.
    ///
    /// Re-resolving "latest" here is the one thing that would break determinism: two nodes
    /// applying the same entry at different times could reach different versions. An entry
    /// without a digest never went through admission, so it is a bug, not an input.
    #[test]
    fn compiling_an_unpinned_binding_is_refused() {
        let mut config = config_with(binding());
        let err = compile_bindings(
            &mut config,
            |_, _| Ok(Some(dataset(9, "latest"))),
            Some(Path::new("/spool")),
        )
        .expect_err("must refuse an unpinned block at apply");
        assert!(
            err.contains("pin") || err.contains("digest"),
            "the refusal must say the block was never pinned: {err}"
        );
    }

    /// A pinned digest that disagrees with the dataset row is refused.
    ///
    /// `(name, version)` is immutable once committed — every upload is a *new* version — so a
    /// disagreement is committed-state corruption. Compiling either side of it would serve rows
    /// nobody bound.
    #[test]
    fn a_digest_disagreeing_with_the_dataset_row_is_refused() {
        let mut b = binding();
        b["version"] = json!(1);
        b["digest"] = json!("pinned");
        let mut config = config_with(b);

        let err = compile_bindings(
            &mut config,
            |_, _| Ok(Some(dataset(1, "something-else"))),
            Some(Path::new("/spool")),
        )
        .expect_err("must refuse");
        assert!(
            err.contains("pinned") || err.contains("digest"),
            "the refusal must name the disagreement: {err}"
        );
    }

    /// E5 — every binding in a config is visited, not just the first.
    ///
    /// Without this a config whose second stub binds an absent dataset would commit, and that
    /// stub would serve unsubstituted `${row}` tokens under a 200.
    #[test]
    fn every_binding_in_the_config_is_pinned_not_only_the_first() {
        let mut config = json!({
            "port": 4545,
            "protocol": "http",
            "stubs": [
                { "responses": [{ "is": {}, "_rift": { "dataset": binding() } }] },
                { "responses": [
                    { "is": {} },
                    { "is": {}, "_rift": { "dataset": binding() } }
                ] }
            ]
        });

        let mut seen = 0;
        pin_bindings(&mut config, |_, _| {
            seen += 1;
            Some(dataset(2, "d2"))
        })
        .expect("pins");

        assert_eq!(seen, 2, "both bindings must be resolved");
        assert_eq!(
            config["stubs"][0]["responses"][0]["_rift"]["dataset"]["digest"],
            json!("d2")
        );
        assert_eq!(
            config["stubs"][1]["responses"][1]["_rift"]["dataset"]["digest"],
            json!("d2")
        );
    }

    /// A binding is found in any body shape, not only a whole `ImposterConfig`.
    ///
    /// The admission path takes five: a config, a replace-all payload, an add-stub body, a
    /// replace-stubs body, and a bare stub. A walk hard-coded to `stubs[].responses[]` would find
    /// the binding in the first and silently miss it in the rest — and a *missed* binding is not
    /// an error at admission, it is an unpinned block that reaches apply and refuses there, so the
    /// operator sees a broken imposter rather than a rejected write.
    #[test]
    fn a_binding_is_found_in_any_body_shape() {
        let shapes = [
            // AddStubBody
            json!({ "stub": { "responses": [{ "is": {}, "_rift": { "dataset": binding() } }] } }),
            // a bare Stub
            json!({ "responses": [{ "is": {}, "_rift": { "dataset": binding() } }] }),
            // ReplaceAllBody — an array of whole configs
            json!({ "imposters": [ config_with(binding()) ] }),
            // the response object on its own
            json!({ "is": {}, "_rift": { "dataset": binding() } }),
        ];

        for (i, shape) in shapes.into_iter().enumerate() {
            let mut document = shape;
            let mut seen = 0;
            pin_bindings(&mut document, |_, _| {
                seen += 1;
                Some(dataset(5, "d5"))
            })
            .expect("pins");
            assert_eq!(seen, 1, "shape {i} must yield exactly one binding");
            assert!(
                document.to_string().contains("\"digest\":\"d5\""),
                "shape {i} must be pinned in place: {document}"
            );
        }
    }

    /// Compiling and stripping are inverses: what an operator gets back is what they wrote.
    ///
    /// Asserted as a round trip rather than by spot-checking, because the property that matters is
    /// that *nothing* of the node-local compilation survives into a rendered document — a
    /// leftover empty `_behaviors` would still be a behaviour the operator never configured, and
    /// would still come back on a `PUT`.
    #[test]
    fn stripping_undoes_compiling_exactly() {
        let mut b = binding();
        b["version"] = json!(1);
        b["digest"] = json!("d1");
        let pinned = config_with(b);

        let mut round_tripped = pinned.clone();
        compile_bindings(
            &mut round_tripped,
            |_, _| Ok(Some(dataset(1, "d1"))),
            Some(Path::new("/spool")),
        )
        .expect("compiles");
        assert!(
            round_tripped.to_string().contains("fromDataSource"),
            "precondition: compiling actually added a lookup"
        );

        strip_compiled(&mut round_tripped, |d| {
            Some(PathBuf::from(format!("/spool/{d}.csv")))
        });
        assert_eq!(round_tripped, pinned);
    }

    /// Compiling works on the shape a **stored config actually has**.
    ///
    /// This is the shape apply sees, and it is not the one the other compile tests use: a stored
    /// config is `serde_json::to_string(&ImposterConfig)`, and upstream's `StubResponseOut`
    /// serializes `behaviors` as an **array**. Every hand-written fixture here used the object
    /// form, which cannot occur in storage — so the array branch was untested and, until this
    /// test, unimplemented. The consequence was not a missed lookup: `desired_configs` turns a
    /// compile refusal into `RefuseSync`, which aborts the sync for **every imposter on the node**.
    /// One ordinary bound stub carrying a `wait` would have frozen the config plane fleet-wide.
    #[test]
    fn compiling_works_on_the_array_shape_a_stored_config_has() {
        let mut b = binding();
        b["version"] = json!(1);
        b["digest"] = json!("d1");
        let mut stored = config_with(b);
        // What upstream serializes: `behaviors` as an array, here already carrying an unrelated
        // behaviour, which is precisely the case that used to refuse.
        stored["stubs"][0]["responses"][0]["behaviors"] = json!([{ "wait": 50 }]);

        compile_bindings(
            &mut stored,
            |_, _| Ok(Some(dataset(1, "d1"))),
            Some(Path::new("/spool")),
        )
        .expect("the array shape must compile, not refuse");

        let entries = stored["stubs"][0]["responses"][0]["behaviors"]
            .as_array()
            .expect("still an array");
        assert_eq!(
            entries[0]["wait"],
            json!(50),
            "the unrelated behaviour survives"
        );
        let lookups: Vec<&Value> = entries.iter().filter_map(|e| e.get("lookup")).collect();
        assert_eq!(lookups.len(), 1, "exactly one lookup element: {stored}");
        assert_eq!(
            lookups[0][0]["fromDataSource"]["csv"]["path"],
            json!("/spool/d1.csv")
        );
    }

    /// A compiled entry joins an existing `lookup` element rather than becoming a second one.
    ///
    /// Upstream's `normalize_behaviors` merges array elements by key with **last wins**, so a
    /// second `{"lookup": …}` element does not add to the first — it replaces it. Appending
    /// blindly would silently delete a hand-written lookup.
    #[test]
    fn compiling_joins_an_existing_lookup_element_instead_of_clobbering_it() {
        let mut b = binding();
        b["version"] = json!(1);
        b["digest"] = json!("d1");
        let mut stored = config_with(b);
        let hand_written = json!({
            "key": { "from": { "query": "x" }, "using": { "method": "regex", "selector": ".*" } },
            "fromDataSource": { "csv": { "path": "/etc/other.csv", "keyColumn": "x" } },
            "into": "${other}"
        });
        stored["stubs"][0]["responses"][0]["behaviors"] =
            json!([{ "lookup": [hand_written.clone()] }]);

        compile_bindings(
            &mut stored,
            |_, _| Ok(Some(dataset(1, "d1"))),
            Some(Path::new("/spool")),
        )
        .expect("compiles");

        let entries = stored["stubs"][0]["responses"][0]["behaviors"]
            .as_array()
            .expect("array");
        assert_eq!(
            entries.len(),
            1,
            "a second element would clobber the first under last-wins merge: {stored}"
        );
        let lookups = entries[0]["lookup"].as_array().expect("lookup array");
        assert_eq!(lookups.len(), 2, "both lookups survive");
        assert_eq!(lookups[0], hand_written);
    }

    /// A storage failure is reported as itself, never as a missing dataset.
    ///
    /// The two were folded together at first: `resolve_dataset` answered `None` for an absent row,
    /// an unreadable table *and* a row that would not parse. A corrupt row then reached the
    /// operator as `dataset "customers" version 7 is not present` — on every node at once, for a
    /// dataset they could still list, with the real cause in no log anywhere. They would chase a
    /// phantom deletion instead of the disk.
    #[test]
    fn a_resolver_failure_is_not_reported_as_an_absent_dataset() {
        let mut b = binding();
        b["version"] = json!(7);
        b["digest"] = json!("d7");
        let mut config = config_with(b);

        let err = compile_bindings(
            &mut config,
            |_, _| Err("stored dataset \"customers\" version 7 will not parse: eof".to_owned()),
            Some(Path::new("/spool")),
        )
        .expect_err("a storage failure is a refusal");
        assert!(
            err.contains("will not parse"),
            "the real cause must survive: {err}"
        );
        assert!(
            !err.contains("not present"),
            "corruption must not be reported as absence: {err}"
        );
    }

    /// Stripping handles the shape the **engine renders**, not only the shape we wrote.
    ///
    /// A config goes in with `behaviors` as an object and comes back out as an *array* of
    /// behaviour objects. Only the written shape was handled at first, so the strip silently did
    /// nothing on exactly the documents it exists to clean — caught by an integration test, and
    /// pinned here so it cannot reopen.
    #[test]
    fn stripping_handles_the_rendered_array_shape() {
        let mut b = binding();
        b["version"] = json!(1);
        b["digest"] = json!("d1");
        let mut rendered = config_with(b);
        rendered["stubs"][0]["responses"][0]["behaviors"] = json!([{
            "lookup": [{
                "key": { "from": { "query": "id" }, "using": { "method": "regex", "selector": ".*" } },
                "fromDataSource": { "csv": {
                    "path": "/spool/d1.csv", "keyColumn": "id", "delimiter": ","
                } },
                "into": "${row}"
            }]
        }]);

        strip_compiled(&mut rendered, |d| {
            Some(PathBuf::from(format!("/spool/{d}.csv")))
        });

        assert!(
            rendered["stubs"][0]["responses"][0]["behaviors"].is_null(),
            "an emptied behaviours array is removed entirely: {rendered}"
        );
        assert_eq!(
            rendered["stubs"][0]["responses"][0]["_rift"]["dataset"]["name"],
            json!("customers"),
            "the operator's own binding survives"
        );
    }

    /// The rendered array shape keeps a hand-written lookup, same as the object shape.
    #[test]
    fn stripping_the_rendered_shape_leaves_a_hand_written_lookup() {
        let mut b = binding();
        b["version"] = json!(1);
        b["digest"] = json!("d1");
        let mut rendered = config_with(b);
        let hand_written = json!({
            "key": { "from": { "query": "x" }, "using": { "method": "regex", "selector": ".*" } },
            "fromDataSource": { "csv": { "path": "/etc/other.csv", "keyColumn": "x" } },
            "into": "${other}"
        });
        rendered["stubs"][0]["responses"][0]["behaviors"] = json!([{
            "lookup": [
                hand_written.clone(),
                { "fromDataSource": { "csv": { "path": "/spool/d1.csv", "keyColumn": "id" } } }
            ]
        }]);

        strip_compiled(&mut rendered, |d| {
            Some(PathBuf::from(format!("/spool/{d}.csv")))
        });

        let entries = rendered["stubs"][0]["responses"][0]["behaviors"][0]["lookup"]
            .as_array()
            .expect("the hand-written lookup keeps the block alive");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], hand_written);
    }

    /// Stripping removes only what this node compiled, never a hand-written lookup.
    #[test]
    fn stripping_leaves_a_hand_written_lookup_alone() {
        let mut b = binding();
        b["version"] = json!(1);
        b["digest"] = json!("d1");
        let mut config = config_with(b);
        let hand_written = json!({
            "key": { "from": { "query": "x" }, "using": { "method": "regex", "selector": ".*" } },
            "fromDataSource": { "csv": { "path": "/etc/other.csv", "keyColumn": "x" } },
            "into": "${other}"
        });
        config["stubs"][0]["responses"][0]["_behaviors"] =
            json!({ "lookup": [hand_written.clone()] });

        compile_bindings(
            &mut config,
            |_, _| Ok(Some(dataset(1, "d1"))),
            Some(Path::new("/spool")),
        )
        .expect("compiles");
        strip_compiled(&mut config, |d| {
            Some(PathBuf::from(format!("/spool/{d}.csv")))
        });

        let entries = lookups(&config).as_array().expect("array");
        assert_eq!(entries.len(), 1, "only the compiled entry is removed");
        assert_eq!(entries[0], hand_written);
    }

    /// E16 — pinning leaves the declarative block in place; it does not compile at admission.
    ///
    /// The stored record is what `GET` renders, and it must show the operator's own binding.
    #[test]
    fn pinning_does_not_compile_the_block_away() {
        let mut config = config_with(binding());
        pin_bindings(&mut config, |_, _| Some(dataset(1, "d1"))).expect("pins");

        assert_eq!(block(&config)["name"], json!("customers"));
        assert_eq!(block(&config)["into"], json!("${row}"));
        assert!(
            config["stubs"][0]["responses"][0]["_behaviors"].is_null(),
            "admission must not compile — the spool path is node-local"
        );
    }
}
