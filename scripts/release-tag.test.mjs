import test from 'node:test';
import assert from 'node:assert/strict';
import { resolveReleaseTag } from './release-tag.mjs';
const sha = 'a'.repeat(40);
test('an absent tag uses the refs API and returns no source', async () => {
  assert.equal(await resolveReleaseTag(async path => {
    assert.equal(path, 'git/ref/tags/v0.1.73');
    return null;
  }, 'v0.1.73'), null);
});
test('a lightweight GitHub reference resolves to its exact commit', async () => {
  assert.deepEqual(await resolveReleaseTag(async () => ({ object: { type: 'commit', sha } }), 'v0.1.73'), { sha });
});
test('an annotated tag is dereferenced before comparing source identity', async () => {
  const calls = [];
  const result = await resolveReleaseTag(async path => {
    calls.push(path);
    return { object: path.startsWith('git/ref/') ? { type: 'tag', sha: 'b'.repeat(40) } : { type: 'commit', sha } };
  }, 'v0.1.73');
  assert.deepEqual(result, { sha });
  assert.equal(calls[1], `git/tags/${'b'.repeat(40)}`);
});
test('cyclic and malformed references cannot become qualified commits', async () => {
  let calls = 0;
  await assert.rejects(resolveReleaseTag(async () => {
    calls += 1;
    return { object: { type: 'tag', sha } };
  }, 'v0.1.73'), /cannot be resolved/);
  assert.equal(calls, 5);
  await assert.rejects(resolveReleaseTag(async () => ({ object: { type: 'commit', sha: 'short' } }), 'v0.1.73'));
});
test('invalid release tags make no upstream requests', async () => {
  await assert.rejects(resolveReleaseTag(async () => { throw Error('unexpected request'); }, '../main'), /Invalid release tag/);
});
