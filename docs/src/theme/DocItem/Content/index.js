import React from "react";
import clsx from "clsx";
import { ThemeClassNames } from "@docusaurus/theme-common";
import { useDoc } from "@docusaurus/plugin-content-docs/client";
import Heading from "@theme/Heading";
import MDXContent from "@theme/MDXContent";
import GitHubCodeLink from "@site/src/components/GitHubCodeLink";
import styles from "./styles.module.css";

function useSyntheticTitle() {
  const { metadata, frontMatter, contentTitle } = useDoc();
  const shouldRender = !frontMatter.hide_title && typeof contentTitle === "undefined";
  if (!shouldRender) {
    return null;
  }
  return metadata.title;
}

export default function DocItemContent({ children }) {
  const { frontMatter } = useDoc();
  const syntheticTitle = useSyntheticTitle();
  const githubPath = frontMatter.github_path;

  return (
    <div className={clsx(ThemeClassNames.docs.docMarkdown, "markdown")}>
      {syntheticTitle && (
        <header className={styles.titleRow}>
          <Heading as="h1" className={styles.title}>
            {syntheticTitle}
          </Heading>
          {githubPath ? <GitHubCodeLink path={githubPath} variant="inline" /> : null}
        </header>
      )}
      <MDXContent>{children}</MDXContent>
    </div>
  );
}
