import React from 'react';
import clsx from 'clsx';
import {blogPostContainerID} from '@docusaurus/utils-common';
import {useBlogPost} from '@docusaurus/plugin-content-blog/client';
import useBaseUrl from '@docusaurus/useBaseUrl';
import MDXContent from '@theme/MDXContent';
import styles from './styles.module.css';

function BlogListPreview({className}) {
  const {metadata, assets} = useBlogPost();
  const {frontMatter, title} = metadata;
  const rawImage = assets.image ?? frontMatter.image;
  const image = useBaseUrl(rawImage);
  const description = frontMatter.description;

  if (!rawImage && !description) {
    return null;
  }

  return (
    <div className={clsx(className, styles.blogListContent, styles.previewWrapper)}>
      {rawImage && (
        <img
          className={styles.previewImage}
          src={image}
          alt={`${title} preview`}
          loading="lazy"
        />
      )}
      {description && <p className={styles.previewDescription}>{description}</p>}
    </div>
  );
}

export default function BlogPostItemContent({children, className}) {
  const {isBlogPostPage} = useBlogPost();

  if (!isBlogPostPage) {
    return <BlogListPreview className={className} />;
  }

  return (
    <div
      // This ID is used for feed generation to locate the main content.
      id={blogPostContainerID}
      className={clsx('markdown', className)}>
      <MDXContent>{children}</MDXContent>
    </div>
  );
}
