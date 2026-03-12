import React from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

const TUVA_REPO_URL = 'https://github.com/tuva-health/tuva';

export default function TuvaCurrentRelease() {
  const {siteConfig} = useDocusaurusContext();
  const version = siteConfig?.customFields?.tuvaVersion || 'latest';

  return <a href={TUVA_REPO_URL}>{version}</a>;
}
