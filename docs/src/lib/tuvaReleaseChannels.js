const TUVA_REPO_URL = 'https://github.com/tuva-health/tuva';
const TUVA_MAIN_BRANCH_URL = `${TUVA_REPO_URL}/tree/main`;
const NO_ACTIVE_RELEASE_CANDIDATE_LABEL = 'No active release candidate';

const DEFAULT_BLEEDING_EDGE = {
  label: 'main',
  url: TUVA_MAIN_BRANCH_URL,
};

function normalizeTag(tag) {
  return String(tag || '').trim();
}

function versionFromTag(tag) {
  return normalizeTag(tag).replace(/^v/i, '');
}

function isRcValue(value) {
  return /-rc$/i.test(normalizeTag(value));
}

function isReleaseCandidate(release) {
  return Boolean(release?.prerelease) ||
    isRcValue(release?.tag_name) ||
    isRcValue(release?.name);
}

function isPublishedRelease(release) {
  return Boolean(release) && !release.draft && Boolean(release.published_at);
}

function isStableRelease(release) {
  return isPublishedRelease(release) && !isReleaseCandidate(release);
}

function releaseTimestamp(release) {
  const timestamp = Date.parse(release?.published_at || '');
  return Number.isNaN(timestamp) ? 0 : timestamp;
}

function formatRelease(release) {
  if (!release) {
    return null;
  }

  const tag = normalizeTag(release.tag_name);

  return {
    version: versionFromTag(tag),
    tag,
    url: release.html_url || `${TUVA_REPO_URL}/releases/tag/${tag}`,
    publishedAt: release.published_at || null,
  };
}

function buildTuvaReleaseChannels(releases) {
  const publishedReleases = [...(releases || [])]
    .filter(isPublishedRelease)
    .sort((left, right) => releaseTimestamp(right) - releaseTimestamp(left));

  const stable = publishedReleases.find(isStableRelease) || null;
  const candidate = publishedReleases.find(isReleaseCandidate) || null;

  return {
    stable: formatRelease(stable),
    candidate: formatRelease(candidate),
    bleedingEdge: DEFAULT_BLEEDING_EDGE,
  };
}

function getCandidateReleaseLabel(candidate) {
  return candidate?.version || NO_ACTIVE_RELEASE_CANDIDATE_LABEL;
}

function pullRequestMergedTimestamp(pullRequest) {
  const timestamp = Date.parse(pullRequest?.merged_at || '');
  return Number.isNaN(timestamp) ? 0 : timestamp;
}

function getLatestMergedPullRequest(pullRequests) {
  return [...(pullRequests || [])]
    .filter((pullRequest) => Boolean(pullRequest?.merged_at))
    .sort(
      (left, right) =>
        pullRequestMergedTimestamp(right) - pullRequestMergedTimestamp(left)
    )[0] || null;
}

function addBleedingEdgePublishedAt(releaseChannels, pullRequests) {
  const latestMergedPullRequest = getLatestMergedPullRequest(pullRequests);

  return {
    ...releaseChannels,
    bleedingEdge: {
      ...DEFAULT_BLEEDING_EDGE,
      ...releaseChannels?.bleedingEdge,
      publishedAt:
        latestMergedPullRequest?.merged_at ||
        releaseChannels?.bleedingEdge?.publishedAt ||
        null,
    },
  };
}

module.exports = {
  DEFAULT_BLEEDING_EDGE,
  NO_ACTIVE_RELEASE_CANDIDATE_LABEL,
  addBleedingEdgePublishedAt,
  buildTuvaReleaseChannels,
  getCandidateReleaseLabel,
  getLatestMergedPullRequest,
  isReleaseCandidate,
  isStableRelease,
  versionFromTag,
};
