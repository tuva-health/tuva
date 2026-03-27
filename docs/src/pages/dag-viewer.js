import Layout from '@theme/Layout';
import styles from './dag-viewer.module.css';

export default function DagViewerPage() {
  return (
    <Layout title="DAG Viewer" description="Read-only DAG snapshots exported from Tuva Enterprise.">
      <main className={styles.page}>
        <iframe
          className={styles.frame}
          src="/dag-viewer-static/index.html"
          title="DAG Viewer"
        />
      </main>
    </Layout>
  );
}
