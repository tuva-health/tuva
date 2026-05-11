const assert = require('node:assert/strict');

const {
  NO_ACTIVE_RELEASE_CANDIDATE_LABEL,
  addBleedingEdgePublishedAt,
  buildTuvaReleaseChannels,
  getCandidateReleaseLabel,
  getLatestMergedPullRequest,
  isReleaseCandidate,
  isStableRelease,
} = require('../src/lib/tuvaReleaseChannels');

const stableRelease = {
  tag_name: 'v0.17.2',
  name: 'the_tuva_project v0.17.2',
  draft: false,
  prerelease: false,
  published_at: '2026-04-02T17:07:35Z',
  html_url: 'https://github.com/tuva-health/tuva/releases/tag/v0.17.2',
};

const rcRelease = {
  tag_name: 'v0.18.0-rc',
  name: 'the_tuva_project v0.18.0-rc',
  draft: false,
  prerelease: true,
  published_at: '2026-04-24T04:35:58Z',
  html_url: 'https://github.com/tuva-health/tuva/releases/tag/v0.18.0-rc',
};

const uppercaseRcRelease = {
  tag_name: 'v0.19.0-RC',
  name: 'the_tuva_project v0.19.0-RC',
  draft: false,
  prerelease: false,
  published_at: '2026-05-08T17:07:35Z',
  html_url: 'https://github.com/tuva-health/tuva/releases/tag/v0.19.0-RC',
};

assert.equal(isReleaseCandidate(rcRelease), true);
assert.equal(isStableRelease(rcRelease), false);

assert.equal(isStableRelease(stableRelease), true);
assert.equal(isReleaseCandidate(stableRelease), false);

assert.equal(isReleaseCandidate(uppercaseRcRelease), true);
assert.equal(isStableRelease(uppercaseRcRelease), false);

const channels = buildTuvaReleaseChannels([
  stableRelease,
  rcRelease,
  uppercaseRcRelease,
]);

assert.equal(channels.stable.version, '0.17.2');
assert.equal(channels.candidate.version, '0.19.0-RC');
assert.equal(channels.bleedingEdge.label, 'main');

const channelsWithoutCandidate = buildTuvaReleaseChannels([stableRelease]);

assert.equal(channelsWithoutCandidate.candidate, null);
assert.equal(
  getCandidateReleaseLabel(channelsWithoutCandidate.candidate),
  NO_ACTIVE_RELEASE_CANDIDATE_LABEL
);

const olderMergedPullRequest = {
  merged_at: '2026-04-29T18:27:06Z',
};
const latestMergedPullRequest = {
  merged_at: '2026-04-29T21:37:44Z',
};
const unmergedPullRequest = {
  merged_at: null,
};

assert.equal(
  getLatestMergedPullRequest([
    unmergedPullRequest,
    olderMergedPullRequest,
    latestMergedPullRequest,
  ]),
  latestMergedPullRequest
);

assert.equal(
  addBleedingEdgePublishedAt(channels, [
    olderMergedPullRequest,
    latestMergedPullRequest,
  ]).bleedingEdge.publishedAt,
  latestMergedPullRequest.merged_at
);

console.log('Release channel classification tests passed.');
