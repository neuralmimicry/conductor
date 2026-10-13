export async function resolveReleaseTag(request, tag) {
  if (!/^v[0-9][0-9A-Za-z.+-]{0,127}$/.test(tag)) throw Error('Invalid release tag');
  // The commits API returns 422 for an absent ref. The exact refs API returns 404.
  const reference = await request(`git/ref/tags/${encodeURIComponent(tag)}`);
  if (!reference) return null;
  let object = reference.object;
  for (let depth = 0; depth < 4; depth += 1) {
    if (!/^[a-f0-9]{40}$/.test(object?.sha)) break;
    if (object.type === 'commit') return { sha: object.sha };
    if (object.type !== 'tag') break;
    object = (await request(`git/tags/${object.sha}`))?.object;
  }
  throw Error('Release tag cannot be resolved to a commit');
}
