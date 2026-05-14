// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion
// {
//   plugins: [require.resolve("docusaurus-plugin-image-zoom")];
// }

const lightCodeTheme = require('prism-react-renderer/dist/index').github;
const darkCodeTheme = require('prism-react-renderer/dist/index').dracula;
const fs = require('fs');
const https = require('https');
const path = require('path');
const {
  DEFAULT_BLEEDING_EDGE,
  addBleedingEdgePublishedAt,
  buildTuvaReleaseChannels,
} = require('./src/lib/tuvaReleaseChannels');
const enableGtag = process.env.ENABLE_DOCS_GTAG === 'true';
const githubReleasesUrl = 'https://api.github.com/repos/tuva-health/tuva/releases?per_page=100';
const githubMergedPullsUrl = 'https://api.github.com/repos/tuva-health/tuva/pulls?state=closed&base=main&sort=updated&direction=desc&per_page=100';
const releaseChannelsFallbackPath = path.resolve(
  __dirname,
  'src/data/tuvaReleaseChannelsFallback.json'
);

function readReleaseChannelsFallback() {
  const fallback = JSON.parse(
    fs.readFileSync(releaseChannelsFallbackPath, 'utf8')
  );

  return {
    ...fallback,
    bleedingEdge: fallback.bleedingEdge || DEFAULT_BLEEDING_EDGE,
  };
}

function requestJson(url) {
  const headers = {
    Accept: 'application/vnd.github+json',
    'User-Agent': 'tuva-docs-release-channel-build',
  };
  const githubToken = process.env.GITHUB_TOKEN || process.env.GH_TOKEN;

  if (githubToken) {
    headers.Authorization = `Bearer ${githubToken}`;
  }

  return new Promise((resolve, reject) => {
    const request = https.get(url, {headers}, (response) => {
      let body = '';

      response.setEncoding('utf8');
      response.on('data', (chunk) => {
        body += chunk;
      });
      response.on('end', () => {
        if (response.statusCode < 200 || response.statusCode >= 300) {
          reject(
            new Error(`GitHub API returned HTTP ${response.statusCode}`)
          );
          return;
        }

        try {
          resolve(JSON.parse(body));
        } catch (error) {
          reject(error);
        }
      });
    });

    request.setTimeout(10000, () => {
      request.destroy(new Error('GitHub release lookup timed out'));
    });
    request.on('error', reject);
  });
}

async function getTuvaReleaseChannels() {
  try {
    const [releases, pullRequests] = await Promise.all([
      requestJson(githubReleasesUrl),
      requestJson(githubMergedPullsUrl),
    ]);
    const releaseChannels = addBleedingEdgePublishedAt(
      buildTuvaReleaseChannels(releases),
      pullRequests
    );

    if (!releaseChannels.stable) {
      throw new Error('GitHub releases response did not include a stable release');
    }

    return releaseChannels;
  } catch (error) {
    console.warn(
      `[docs] Unable to fetch Tuva GitHub releases. Using fallback release channels. ${error.message}`
    );
    return readReleaseChannelsFallback();
  }
}

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'The Tuva Project',
  tagline: 'Open source software for transforming raw healthcare data',
  url: 'https://www.thetuvaproject.com',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  stylesheets: [require.resolve('./src/css/custom.css')],
  staticDirectories: [
    'static',
    path.resolve(__dirname, '../models'),
    path.resolve(__dirname, '../models/input_layer'),
    path.resolve(__dirname, '../models/core'),
  ],

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'tuva-health', // Usually your GitHub org/user name.
  projectName: 'docs', // Usually your repo name.
  deploymentBranch: 'deployment',
  trailingSlash: false,

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },
  customFields: {
  },

  themes: ['@docusaurus/theme-mermaid'],
  markdown: { mermaid: true },

  plugins: [
    require.resolve("docusaurus-plugin-image-zoom"),
    [
      '@docusaurus/plugin-client-redirects',
      {
        redirects: [
          // Renamed files from PR #493
          {
            from: '/data-quality',
            to: '/data-pipeline-tests',
          },
          {
            from: '/core-data-model/overview',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/appointment',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/condition',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/eligibility',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/encounter',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/immunization',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/lab-result',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/location',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/medical-claim',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/medication',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/member-months',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/observation',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/patient',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/person_id_crosswalk',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/pharmacy-claim',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/practitioner',
            to: '/core-data-model',
          },
          {
            from: '/core-data-model/procedure',
            to: '/core-data-model',
          },
          {
            from: '/tuva-empi',
            to: '/empi',
          },
          
          // Moved to archive
          {
            from: '/getting-started/geo-coding-sdoh',
            to: '/archive/geo-coding-sdoh',
          },
          {
            from: '/videos/community',
            to: '/archive/videos/community',
          },
          {
            from: '/videos/guides',
            to: '/archive/videos/guides',
          },
          {
            from: '/videos/knowledge',
            to: '/archive/videos/knowledge',
          },
          
          // Deleted use-cases pages - redirect to relevant data-marts pages
          {
            from: '/use-cases/overview',
            to: '/data-marts/overview',
          },
          {
            from: '/use-cases/acute-inpatient',
            to: '/knowledge/analytics/acute-ip-visits',
          },
          {
            from: '/use-cases/ahrq-measures',
            to: '/data-marts/ahrq-measures',
          },
          {
            from: '/use-cases/chronic-conditions',
            to: '/data-marts/chronic-conditions',
          },
          {
            from: '/use-cases/cms-hccs',
            to: '/data-marts/cms-hccs',
          },
          {
            from: '/use-cases/demographics',
            to: '/core-data-model',
          },
          {
            from: '/use-cases/ed-visits',
            to: '/knowledge/analytics/ed-visits',
          },
          {
            from: '/use-cases/medical-pmpm',
            to: '/data-marts/financial-pmpm',
          },
          {
            from: '/use-cases/pharmacy',
            to: '/data-marts/pharmacy',
          },
          {
            from: '/use-cases/primary-care',
            to: '/knowledge/analytics/utilization-metrics',
          },
          {
            from: '/use-cases/urgent-care',
            to: '/knowledge/analytics/utilization-metrics',
          },
          
          // Deleted analytics pages - redirect to knowledge/analytics
          {
            from: '/analytics/overview',
            to: '/knowledge/analytics/utilization-metrics',
          },
          {
            from: '/analytics/notebooks',
            to: '/notebooks',
          },
          {
            from: '/analytics/streamlit',
            to: '/dashboards',
          },
          {
            from: '/analytics/dashboards',
            to: '/dashboards',
          },
          
          // Deleted guides pages - redirect to relevant new pages
          {
            from: '/guides/data-source-setup/ingestion',
            to: '/input-layer',
          },
          {
            from: '/connectors/input-layer',
            to: '/input-layer',
          },
          {
            from: '/guides/data-source-setup/audit',
            to: '/data-pipeline-tests',
          },
          {
            from: '/guides/data-source-setup/deployment',
            to: '/getting-started',
          },
          {
            from: '/guides/mapping/fhir',
            to: '/connectors/fhir-inferno',
          },
          
          // Deleted getting-started pages
          {
            from: '/getting-started/customizations',
            to: '/getting-started',
          },
          {
            from: '/getting-started/synthetic-data-demo',
            to: '/getting-started',
          },
          {
            from: '/archive/getting-started/contributing',
            to: '/getting-started',
          },
          {
            from: '/getting-started/contributing',
            to: '/getting-started',
          },
          {
            from: '/contributing',
            to: '/getting-started',
          },
          
          // Deleted more pages
          {
            from: '/more/data-stories',
            to: '/knowledge/introduction',
          },
          {
            from: '/more/videos',
            to: '/archive/videos/knowledge',
          },
        ],
        createRedirects(existingPath) {
          // Pattern-based redirects for any remaining value-sets pages
          if (existingPath.includes('/archive/value-sets/')) {
            return [
              existingPath.replace('/archive/value-sets/', '/value-sets/'),
            ];
          }
          return undefined;
        },
      },
    ],
  ],

  presets: [
    [
       'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      {
        docs: {
          path: 'docs',
          routeBasePath: "/",
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl:
            'https://github.com/tuva-health/docs/edit/main/'
        },
        blog: false,

        // blog: {
        //   blogTitle: 'Decoding Healthcare Analytics',
        //   blogDescription: 'A Docusaurus powered blog!',
        //   postsPerPage: 'ALL',
        //   blogSidebarTitle: 'All posts',
        //   blogSidebarCount: 'ALL',
        //   showReadingTime: true,
        // },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        ...(enableGtag
          ? {
              gtag: {
                // Google Analytics 4 https://developers.google.com/analytics/devguides/collection/gtagjs/
                trackingID: 'G-2FG30MEX5P',
                anonymizeIP: false,
              },
            }
          : {}),
      },
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: '',
        logo: {
          alt: 'The Tuva Project',
          src: 'img/the_tuva_project_logo_new.png' /*'img/TheTuvaProjectLogo.png',*/
        },
        items: [

          {
            type: 'doc',
            docId: 'welcome',
            position: 'left',
            label: 'Docs',
          },
          {
            type: 'docSidebar',
            sidebarId: 'knowledgeSidebar',
            position: 'left',
            label: 'Knowledge',
            collapsed: false,
          },
          {
            type: 'docSidebar',
            sidebarId: 'communitySidebar',
            position: 'left',
            label: 'Community',
          },
          {
            href: 'https://thetuvaproject.substack.com/',
            label: 'Newsletter',
            position: 'left',
            className: 'navbar-no-ext-icon',
            hideExternalLinkIcon: true,
          },
          {
            href: 'https://www.thetuvaproject.com/blog/',
            label: 'Blog',
            position: 'left',
            className: 'navbar-no-ext-icon',
            hideExternalLinkIcon: true,
          },          // {
          //   type: 'docSidebar',
          //   sidebarId: 'moreSidebar',
          //   position: 'left',
          //   label: '+ More',
          // },
          // {
          //   type: 'docSidebar',
          //   sidebarId: 'videoSidebar',
          //   position: 'left',
          //   label: 'Videos',
          // },
          // { to: 'journal-club', label: 'Journal Club', position: 'left' },
          // { to: 'manifesto', label: 'Manifesto', position: 'left' },
          {
            href: 'https://dagviewer.thetuvaproject.com',
            position: 'right',
            label: 'DAG Viewer',
            className: 'navbar-no-ext-icon',
            hideExternalLinkIcon: true,
          },
          {
            href: 'https://tuvahealth.com',
            position: 'right',
            className: 'header-tuva-link',
            'aria-label': 'Tuva Health',
          },
          {
            href: 'https://www.youtube.com/@thetuvaproject',
            position: 'right',
            className: 'header-youtube-link',
            'aria-label': 'YouTube',
          },
          {
            href: 'https://join.slack.com/t/thetuvaproject/shared_invite/zt-35dtjo7sz-QnTKVZJaEErr35cxFZ2QMA',
            position: 'right',
            className: 'header-slack-link',
            'aria-label': 'Slack Community',
          },
          {
            href: 'https://github.com/tuva-health',
            position: 'right',
            className: 'header-github-link',
            'aria-label': 'GitHub repository',
          },
          // {to: '/blog', label: 'Blog', position: 'left'}

        ],

      },

      footer: {
        style: 'light',
      //   links: [
      //     {
      //       title: 'Docs',
      //       items: [
      //         {
      //           label: 'Tutorial',
      //           to: '/docs/intro',
      //         },
      //       ],
      //     },
      //     {
      //       title: 'Community',
      //       items: [
      //         {
      //           label: 'Stack Overflow',
      //           href: 'https://stackoverflow.com/questions/tagged/docusaurus',
      //         },
      //         {
      //           label: 'Discord',
      //           href: 'https://discordapp.com/invite/docusaurus',
      //         },
      //         {
      //           label: 'Twitter',
      //           href: 'https://twitter.com/docusaurus',
      //         },
      //       ],
      //     },
      //     {
      //       title: 'More',
      //       items: [
      //         {
      //           label: 'Blog',
      //           to: '/blog',
      //         },
      //         {
      //           label: 'GitHub',
      //           href: 'https://github.com/facebook/docusaurus',
      //         },
      //       ],
      //     },
      //   ],
        copyright: `<a href="https://netlify.com">This site is powered by Netlify</a> &nbsp;&nbsp;&nbsp; Copyright © ${new Date().getFullYear()} The Tuva Project`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
        zoom: {
          selector: '.markdown :not(em) > img',
          config: {
            // options you can specify via https://github.com/francoischalifour/medium-zoom#usage
            background: {
              light: 'rgb(255, 255, 255)',
              dark: 'rgb(50, 50, 50)'
            }
          }
        },
        docs: {
          sidebar: {
            hideable: true,
            autoCollapseCategories: true
          },

        },
        blog: {
          // Use a custom truncate marker
          //truncateMarker: /<!-- truncate -->/,
        },
    }),
};

module.exports = async function createConfig() {
  const tuvaReleaseChannels = await getTuvaReleaseChannels();

  return {
    ...config,
    customFields: {
      ...config.customFields,
      tuvaVersion: tuvaReleaseChannels.stable?.version || 'latest',
      tuvaReleaseChannels,
    },
  };
};
