import React from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

const TUVA_RELEASES_URL = 'https://github.com/tuva-health/tuva/releases';

export default function TuvaCurrentRelease() {
  const {siteConfig} = useDocusaurusContext();
  const releaseChannels = siteConfig?.customFields?.tuvaReleaseChannels;
  const stableRelease = releaseChannels?.stable;
  const version =
    stableRelease?.version || siteConfig?.customFields?.tuvaVersion || 'latest';
  const url = stableRelease?.url || TUVA_RELEASES_URL;

  return <a href={url}>{version}</a>;
}
