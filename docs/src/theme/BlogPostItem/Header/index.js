import React from 'react';
import {useBlogPost} from '@docusaurus/plugin-content-blog/client';
import BlogPostItemHeaderTitle from '@theme/BlogPostItem/Header/Title';
import BlogPostItemHeaderInfo from '@theme/BlogPostItem/Header/Info';
import BlogPostItemHeaderAuthors from '@theme/BlogPostItem/Header/Authors';
import styles from './styles.module.css';

export default function BlogPostItemHeader() {
  const {metadata, isBlogPostPage} = useBlogPost();
  const subtitle = metadata.frontMatter?.subtitle;

  return (
    <header>
      <BlogPostItemHeaderTitle />
      {isBlogPostPage && subtitle && <p className={styles.subtitle}>{subtitle}</p>}
      <BlogPostItemHeaderInfo />
      <BlogPostItemHeaderAuthors />
    </header>
  );
}
