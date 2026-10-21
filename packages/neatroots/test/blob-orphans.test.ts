import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { afterEach, describe, expect, it } from "vitest";
import { ORPHAN_GRACE_MS } from "../src/store/blobs.js";
import { Store } from "../src/store/db.js";
import { batch, hex, s, span, tmpHome } from "./helpers.js";

const homes: string[] = [];
function dataDir(): string {
  const h = tmpHome();
  homes.push(h);
  return path.join(h, "data");
}
afterEach(() => {
  for (const h of homes.splice(0)) fs.rmSync(h, { recursive: true, force: true });
});

const longAgo = () => new Date(Date.now() - 2 * ORPHAN_GRACE_MS);
const age = (file: string) => fs.utimesSync(file, longAgo(), longAgo());
const fileOf = (st: Store, sha: string) => path.join(path.join(st.dir, "blobs"), sha.slice(0, 2), sha.slice(2));
const big = (tag: string) => tag + "x".repeat(100_000);

describe("blob files left by an interrupted ingest", () => {
  it("are removed on the next start, and stop counting toward the size", () => {
    const dir = dataDir();
    const st = new Store(dir);
    const t = hex(16);
    st.ingest([batch([span({ traceId: t, name: "kept", kind: "tool", attrs: [s("neatlogs.tool.output", big("kept"))] })])]);
    const keptBytes = st.blobs.size();
    // A crash between writing a blob and committing its span leaves the payload with nothing referencing it.
    const orphan = fileOf(st, st.blobs.put(big("orphan")));
    const tmp = `${orphan}.4242.deadbeef.tmp`; // what an interrupted write leaves next to its final name
    fs.writeFileSync(tmp, "half a payload");
    expect(st.blobs.size()).toBeGreaterThan(keptBytes);
    for (const f of [orphan, tmp]) age(f);
    st.close();

    const reopened = new Store(dir);
    expect(fs.existsSync(orphan)).toBe(false);
    expect(fs.existsSync(tmp)).toBe(false);
    expect(reopened.blobs.size()).toBe(keptBytes);
    reopened.close();
  });

  it("keep every payload a span still references", () => {
    const dir = dataDir();
    const st = new Store(dir);
    const t = hex(16);
    const sp = span({ traceId: t, name: "kept", kind: "tool", attrs: [s("neatlogs.tool.output", big("kept"))] });
    st.ingest([batch([sp])]);
    const [row] = st.db.prepare("SELECT blobs FROM spans").all() as { blobs: string }[];
    for (const sha of JSON.parse(row.blobs) as string[]) age(fileOf(st, sha));
    st.close();

    const reopened = new Store(dir);
    expect(reopened.batchesForTraces([t])[0].spans[0].attributes).toEqual(sp.attributes);
    reopened.close();
  });

  it("are kept while recent, since an ingest writes its blobs before its span commits", () => {
    const dir = dataDir();
    const st = new Store(dir);
    const inFlight = fileOf(st, st.blobs.put(big("in flight")));
    const tmp = `${inFlight}.1.0badf00d.tmp`;
    fs.writeFileSync(tmp, "being written");
    st.close();

    const reopened = new Store(dir);
    expect(fs.existsSync(inFlight)).toBe(true);
    expect(fs.existsSync(tmp)).toBe(true);
    reopened.close();
  });

  it("leave files the store did not write alone", () => {
    const dir = dataDir();
    const st = new Store(dir);
    const stray = path.join(dir, "blobs", "notes.txt");
    const strayShard = path.join(dir, "blobs", "zz", "keep-me");
    fs.mkdirSync(path.dirname(strayShard), { recursive: true });
    fs.writeFileSync(stray, "x");
    fs.writeFileSync(strayShard, "x");
    age(stray);
    age(strayShard);
    st.close();

    new Store(dir).close();
    expect(fs.existsSync(stray)).toBe(true);
    expect(fs.existsSync(strayShard)).toBe(true);
  });

  it("are not swept while another connection holds the write lock, as an ingest does between its blob writes and its commit", () => {
    const dir = dataDir();
    const st = new Store(dir);
    const orphan = fileOf(st, st.blobs.put(big("old orphan")));
    age(orphan);
    const sweep = () => (st as unknown as { reclaimOrphanBlobs(): void }).reclaimOrphanBlobs();
    // An ingest in flight: it holds the write lock, and its batch is about to reuse the old payload.
    const ingest = new Database(st.file);
    ingest.exec("BEGIN IMMEDIATE");

    sweep(); // must give up, not read references and delete under the ingest
    expect(fs.existsSync(orphan)).toBe(true);

    ingest.exec("COMMIT");
    ingest.close();
    sweep(); // nothing holds the lock now
    expect(fs.existsSync(orphan)).toBe(false);
    st.close();
  });

  it("do not follow a shard that is a symlink out of the store", (ctx) => {
    const dir = dataDir();
    const st = new Store(dir);
    const elsewhere = path.join(path.dirname(dir), "elsewhere");
    fs.mkdirSync(elsewhere);
    const victim = path.join(elsewhere, "a".repeat(62));
    fs.writeFileSync(victim, "not this store's");
    age(victim);
    try {
      fs.symlinkSync(elsewhere, path.join(dir, "blobs", "ab"), "dir");
    } catch {
      ctx.skip(); // no permission to create symlinks here
    }
    st.close();

    new Store(dir).close();
    expect(fs.existsSync(victim)).toBe(true);
  });
});
