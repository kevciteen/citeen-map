/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  serverExternalPackages: ["better-sqlite3"],
  experimental: {
    optimizePackageImports: ["lucide-react"],
  },
};

export default nextConfig;
