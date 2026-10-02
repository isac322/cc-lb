import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';
import starlight from '@astrojs/starlight';

const siteUrl = 'https://cc-lb.bhyoo.com';
const structuredData = [
  {
    tag: 'script',
    attrs: { type: 'application/ld+json' },
    content: JSON.stringify({
      '@context': 'https://schema.org',
      '@type': 'SoftwareApplication',
      name: 'cc-lb',
      applicationCategory: 'DeveloperApplication',
      operatingSystem: 'Linux',
      description:
        'Self-hosted Anthropic-compatible reverse proxy and load balancer for pooled API-key and OAuth upstreams.',
      url: siteUrl,
      codeRepository: 'https://github.com/isac322/cc-lb',
      license: 'https://www.apache.org/licenses/LICENSE-2.0',
    }),
  },
  {
    tag: 'script',
    attrs: { type: 'application/ld+json' },
    content: JSON.stringify({
      '@context': 'https://schema.org',
      '@type': 'SoftwareSourceCode',
      name: 'cc-lb',
      codeRepository: 'https://github.com/isac322/cc-lb',
      programmingLanguage: 'Rust',
      runtimePlatform: 'Linux',
      license: 'https://www.apache.org/licenses/LICENSE-2.0',
      targetProduct: {
        '@type': 'SoftwareApplication',
        name: 'cc-lb',
      },
    }),
  },
];

export default defineConfig({
  site: siteUrl,
  output: 'static',
  integrations: [
    sitemap(),
    starlight({
      favicon: '/brand/favicon/favicon.svg',
      title: 'cc-lb documentation',
      description:
        'Operate a self-hosted Anthropic-compatible reverse proxy and load balancer.',
      logo: {
        light: './src/assets/cc-lb-lockup-horizontal-on-light.svg',
        dark: './src/assets/cc-lb-lockup-horizontal-on-dark.svg',
        alt: 'cc-lb',
        replacesTitle: true,
      },
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/isac322/cc-lb' },
      ],
      customCss: ['./src/styles/custom.css'],
      head: [
        { tag: 'link', attrs: { rel: 'icon', href: '/brand/favicon/favicon.ico', sizes: '32x32' } },
        { tag: 'link', attrs: { rel: 'apple-touch-icon', href: '/brand/favicon/apple-touch-icon.png' } },
        { tag: 'link', attrs: { rel: 'manifest', href: '/manifest.webmanifest' } },
        { tag: 'meta', attrs: { name: 'theme-color', content: '#161719' } },
        { tag: 'meta', attrs: { property: 'og:site_name', content: 'cc-lb' } },
        { tag: 'meta', attrs: { property: 'og:type', content: 'website' } },
        { tag: 'meta', attrs: { property: 'og:image', content: `${siteUrl}/brand/social/cc-lb-social-preview.png` } },
        { tag: 'meta', attrs: { name: 'twitter:card', content: 'summary_large_image' } },
        { tag: 'meta', attrs: { name: 'twitter:image', content: `${siteUrl}/brand/social/cc-lb-social-preview.png` } },
        ...structuredData,
      ],
      sidebar: [
        { label: 'Overview', items: [{ label: 'Documentation home', slug: 'docs' }] },
        {
          label: 'Getting started',
          items: [
            { label: 'Run cc-lb', slug: 'docs/getting-started' },
            { label: 'Install and configure', slug: 'docs/getting-started/install' },
          ],
        },
        {
          label: 'Concepts',
          items: [
            { label: 'Routing model', slug: 'docs/concepts' },
            { label: 'Upstreams', slug: 'docs/concepts/upstreams' },
            { label: 'Prompt-cache continuity', slug: 'docs/concepts/prompt-cache' },
            { label: 'Quota routing', slug: 'docs/concepts/quota-routing' },
          ],
        },
        {
          label: 'Operations',
          items: [
            { label: 'Operator overview', slug: 'docs/operations' },
            { label: 'Runtime management', slug: 'docs/operations/runtime-management' },
            { label: 'Scheduler and warm-up', slug: 'docs/operations/scheduler' },
            { label: 'Observability', slug: 'docs/operations/observability' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'Reference overview', slug: 'docs/reference' },
            { label: 'Configuration', slug: 'docs/reference/configuration' },
            { label: 'Admin API', slug: 'docs/reference/admin-api' },
            { label: 'Metrics', slug: 'docs/reference/metrics' },
            { label: 'Curated source notes', slug: 'docs/reference/source/runtime-management' },
            { label: 'Scheduler source notes', slug: 'docs/reference/source/scheduler' },
            { label: 'Upstream warm-up source notes', slug: 'docs/reference/source/upstream-warmup' },
          ],
        },
        {
          label: 'Plugins',
          items: [
            { label: 'Plugin overview', slug: 'docs/plugins' },
            { label: 'Author a plugin', slug: 'docs/plugins/author-guide' },
            { label: 'Conformance', slug: 'docs/plugins/conformance' },
            { label: 'Plugin author source notes', slug: 'docs/reference/source/plugin-author-guide' },
          ],
        },
        {
          label: 'Trust',
          items: [
            { label: 'Trust and scope', slug: 'docs/trust' },
            { label: 'Security', slug: 'docs/trust/security' },
            { label: 'License and provenance', slug: 'docs/trust/license' },
          ],
        },
      ],
    }),
  ],
});
