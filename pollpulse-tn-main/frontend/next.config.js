/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // Required for react-leaflet to work properly
  transpilePackages: ['react-leaflet'],
}

module.exports = nextConfig
