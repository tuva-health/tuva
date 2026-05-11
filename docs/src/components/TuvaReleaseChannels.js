import React from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {
  DEFAULT_BLEEDING_EDGE,
  NO_ACTIVE_RELEASE_CANDIDATE_LABEL,
  getCandidateReleaseLabel,
} from '@site/src/lib/tuvaReleaseChannels';
import styles from './TuvaReleaseChannels.module.css';

function formatPublishedAt(publishedAt) {
  if (!publishedAt) {
    return null;
  }

  const publishedDate = new Date(publishedAt);

  if (Number.isNaN(publishedDate.getTime())) {
    return null;
  }

  return new Intl.DateTimeFormat('en', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC',
  }).format(publishedDate);
}

function ReleaseLink({href, label}) {
  if (!href) {
    return <span className={styles.releaseValue}>{label}</span>;
  }

  return (
    <a className={styles.releaseValue} href={href}>
      {label}
    </a>
  );
}

function ReleaseChannel({description, href, label, publishedAt, title}) {
  const formattedDate = formatPublishedAt(publishedAt);

  return (
    <article className={styles.channel}>
      <div className={styles.channelHeader}>
        <h3 className={styles.channelTitle}>{title}</h3>
        {formattedDate ? (
          <span className={styles.publishedDate}>{formattedDate}</span>
        ) : null}
      </div>
      <ReleaseLink href={href} label={label} />
      <p className={styles.channelDescription}>{description}</p>
    </article>
  );
}

export default function TuvaReleaseChannels() {
  const {siteConfig} = useDocusaurusContext();
  const releaseChannels = siteConfig?.customFields?.tuvaReleaseChannels || {};
  const stable = releaseChannels.stable;
  const candidate = releaseChannels.candidate;
  const bleedingEdge = releaseChannels.bleedingEdge || DEFAULT_BLEEDING_EDGE;
  const candidateLabel = getCandidateReleaseLabel(candidate);

  return (
    <div className={styles.releaseChannels}>
      <ReleaseChannel
        description="Recommended for production dbt package installs."
        href={stable?.url}
        label={stable?.version || 'latest'}
        publishedAt={stable?.publishedAt}
        title="Most recent stable release"
      />
      <ReleaseChannel
        description="Upcoming release validation build."
        href={candidate ? candidate.url : null}
        label={candidateLabel}
        publishedAt={candidate?.publishedAt}
        title="Release candidate"
      />
      <ReleaseChannel
        description="Latest merged code on GitHub."
        href={bleedingEdge.url}
        label={bleedingEdge.label || 'main'}
        publishedAt={bleedingEdge.publishedAt}
        title="Bleeding edge"
      />
    </div>
  );
}

export {NO_ACTIVE_RELEASE_CANDIDATE_LABEL};
